package database

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tebben/geocodeur/settings"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	log "github.com/sirupsen/logrus"
)

// put it here for now, move to settings and load
var TABLE_OVERTURE = "overture"
var TABLE_SEARCH = "overture_search"
var aliases = map[string]string{
	"'s-Hertogenbosch": "Den Bosch",
}
var truncations = []string{
	"Rijksweg",
}

var counter uint64

type Record struct {
	ID       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Geom     string `parquet:"name=geom, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Class    string `parquet:"name=class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Subclass string `parquet:"name=subclass, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Relation string `parquet:"name=relation, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
}

func getNextID() uint64 {
	return atomic.AddUint64(&counter, 1)
}

func CreateDB(config settings.Config) {
	pool, err := GetDBPool("geocodeur", config.Database)
	if err != nil {
		log.Fatalf("Failed to get database pool: %v", err)
	}

	log.Infof("Setting up database %s", config.Database.Schema)
	err = setupDatabase(pool, config.Database.Schema)
	if err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	log.Infof("Creating tables %s and %s", TABLE_OVERTURE, TABLE_SEARCH)
	err = createTableOverture(pool, config.Database.Tablespace)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	err = createTableSearch(pool, config.Database.Tablespace)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_division.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_segment.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_water.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_poi.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_infra.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_address.parquet"))
	processParquet(pool, fmt.Sprintf("%s%s", config.Process.Folder, "geocodeur_zipcode.parquet"))

	log.Info("Creating foreign key overture_search -> overture")
	err = createForeignKey(pool)
	if err != nil {
		log.Fatalf("Failed to create foreign key: %v", err)
	}

	log.Info("Creating overture geom index")
	err = createIndexGeom(pool)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	log.Info("Creating search rank index")
	err = createIndexRank(pool)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	log.Info("Creating search trgm index")
	err = createIndexTrgm(pool)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	log.Info("Creating fts column")
	err = createFTSVectorColumn(pool)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	log.Info("Running full vacuum")
	err = vacuum(pool)
	if err != nil {
		log.Fatalf("Failed to vacuum table: %v", err)
	}
}

func processParquet(pool *pgxpool.Pool, path string) {
	log.Infof("Inserting %s\n", path)

	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(Record), 4)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	batchSize := 100
	sem := make(chan struct{}, 10) // Limit to 10 concurrent goroutines

	var wg sync.WaitGroup
	for {
		records := make([]Record, batchSize)
		var num int
		if err := pr.Read(&records); err != nil {
			log.Fatalf("Failed to read records: %v", err)
		}
		num = len(records)
		if num == 0 {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // Block if there are already 10 goroutines
		go func(records []Record) {
			defer wg.Done()
			defer func() { <-sem }() // Release a slot in the semaphore

			tx, err := pool.Begin(context.Background())
			if err != nil {
				log.Fatalf("Failed to begin transaction: %v", err)
			}
			defer tx.Rollback(context.Background())

			for _, rec := range records {
				if rec.Name == "" {
					continue
				}
				id := getNextID()
				err := addOvertureFeature(tx, rec, id)
				if err != nil {
					log.Printf("Failed to insert record: %v", err)
				}

				// Process aliases
				processAliases(tx, rec, id)
			}

			if err := tx.Commit(context.Background()); err != nil {
				log.Fatalf("Failed to commit transaction: %v", err)
			}
		}(records[:num])
	}

	wg.Wait()
	log.Infof("Inserted %s\n", path)
}

func processAliases(tx pgx.Tx, rec Record, id uint64) {
	// Add name as alias
	addAlias(tx, rec, rec.Name, id)

	// Add aliases for name aliases
	for name, alias := range aliases {
		if rec.Name == name {
			addAlias(tx, rec, alias, id)
		}
	}

	// Add embedding for truncated names
	for _, truncation := range truncations {
		if strings.Contains(rec.Name, truncation) {
			alias := strings.Trim(strings.Replace(rec.Name, truncation, "", 1), " ")
			addAlias(tx, rec, alias, id)
		}
	}

	// Add alias for name + relation
	if len(rec.Relation) > 0 {
		relations := strings.Split(rec.Relation, ";")
		for _, relation := range relations {
			if rec.Name == relation {
				continue
			}

			alias := rec.Name + " " + relation
			addAlias(tx, rec, alias, id)

			// Add entry for relation aliases
			for name, alias := range aliases {
				if relation == name {
					aliasEmbedding := rec.Name + " " + alias
					addAlias(tx, rec, aliasEmbedding, id)
				}
			}
		}
	}
}

func addOvertureFeature(tx pgx.Tx, rec Record, recordId uint64) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, name, class, subclass, divisions, geom) VALUES ($1, $2, $3, $4, string_to_array($5, ';'), ST_GeomFromText($6, 4326));`, TABLE_OVERTURE)
	_, err := tx.Exec(context.Background(), query, recordId, rec.Name, rec.Class, rec.Subclass, rec.Relation, rec.Geom)

	return err
}

func addAlias(tx pgx.Tx, rec Record, alias string, recordId uint64) error {
	alias = strings.ToLower(alias)
	classRank := getClassRank(rec.Class)
	subclassRank := getSubclassScore(rec.Subclass)
	wordCount := len(strings.Split(alias, " "))
	charCount := len(alias)

	query := fmt.Sprintf(`INSERT INTO %s (feature_id, alias, class_rank, subclass_rank, word_count, char_count) VALUES ($1, $2, $3, $4, $5, $6)`, TABLE_SEARCH)
	_, err := tx.Exec(context.Background(), query, recordId, alias, classRank, subclassRank, wordCount, charCount)

	return err
}

func vacuum(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), fmt.Sprintf("VACUUM FULL %s;", TABLE_OVERTURE))
	if err != nil {
		return fmt.Errorf("failed to vacuum table %s: %v", TABLE_OVERTURE, err)
	}

	_, err = pool.Exec(context.Background(), fmt.Sprintf("VACUUM FULL %s;", TABLE_SEARCH))
	if err != nil {
		return fmt.Errorf("failed to vacuum table %s: %v", TABLE_SEARCH, err)
	}

	return nil
}

func setupDatabase(pool *pgxpool.Pool, schema string) error {
	if schema != "" {
		_, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema))
		if err != nil {
			return fmt.Errorf("failed to create schema: %v", err)
		}
	}

	queryExtensions := `
		CREATE EXTENSION IF NOT EXISTS postgis;
		CREATE EXTENSION IF NOT EXISTS pg_trgm;
	`

	_, err := pool.Exec(context.Background(), queryExtensions)
	if err != nil {
		return fmt.Errorf("failed to create extensions: %v", err)
	}

	return nil
}

func createTableOverture(pool *pgxpool.Pool, tablespace string) error {
	if tablespace != "" {
		tablespace = fmt.Sprintf("TABLESPACE %s", tablespace)
	}

	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %[1]s CASCADE;
		DROP INDEX IF EXISTS idx_%[1]s_geom;

		CREATE TABLE %[1]s (
			id BIGINT PRIMARY KEY,
			name TEXT,
			class TEXT,
			subclass TEXT,
			divisions TEXT[],
			geom geometry(Geometry, 4326)
		) %s;
	`, TABLE_OVERTURE, tablespace)

	_, err := pool.Exec(context.Background(), query)
	return err
}

// Recreate the table in PostgreSQL
func createTableSearch(pool *pgxpool.Pool, tablespace string) error {
	if tablespace != "" {
		tablespace = fmt.Sprintf("TABLESPACE %s", tablespace)
	}

	query := fmt.Sprintf(`
        CREATE EXTENSION IF NOT EXISTS pg_trgm;

        DROP TABLE IF EXISTS %[1]s;
        DROP INDEX IF EXISTS idx_%[1]s_trgm;
		DROP INDEX IF EXISTS idx_%[1]s_vector_search;

        CREATE TABLE %[1]s (
			feature_id BIGINT,
            alias TEXT,
			class_rank INT,
			subclass_rank INT,
			word_count INT,
			char_count INT
        ) %[2]s;
    `, TABLE_SEARCH, tablespace)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func createForeignKey(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		ALTER TABLE %[1]s ADD CONSTRAINT fk_%[1]s_feature_id FOREIGN KEY (feature_id) REFERENCES %[2]s (id) ON DELETE CASCADE;
	`, TABLE_SEARCH, TABLE_OVERTURE)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func createIndexRank(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_%[1]s_class_subclass ON %[1]s USING btree (class_rank, subclass_rank);
	`, TABLE_SEARCH)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func createIndexGeom(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_%[1]s_geom ON %[1]s USING GIST (geom);
	`, TABLE_OVERTURE)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func createIndexTrgm(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_%[1]s_trgm ON %[1]s USING gin (alias gin_trgm_ops);
	`, TABLE_SEARCH)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func createFTSVectorColumn(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		ALTER TABLE %[1]s ADD COLUMN vector_search tsvector;
		UPDATE %[1]s SET vector_search = to_tsvector('simple', alias);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_vector_search ON %[1]s USING GIN (vector_search);
	`, TABLE_SEARCH)

	_, err := pool.Exec(context.Background(), query)
	return err
}

func getClassRank(class string) int {
	switch class {
	case "division":
		return 1
	case "water": // lot of division names with partly water name, maas, ijssel, etc, rank the same
		return 1
	case "road":
		return 2
	case "infra":
		return 3
	case "address":
		return 4
	case "zipcode":
		return 5
	case "poi":
		return 6
	default:
		return 100
	}
}

func getSubclassScore(subclass string) int {
	switch subclass {
	case "locality":
		return 1
	case "county":
		return 2
	case "neighboorhood":
		return 3
	case "microhood":
		return 4
	case "motorway":
		return 1
	case "trunk":
		return 2
	case "primary":
		return 3
	case "secondary":
		return 4
	case "tertiary":
		return 5
	case "unclassified":
		return 6
	case "residential":
		return 6
	case "living_street":
		return 6
	default:
		return 100
	}
}
