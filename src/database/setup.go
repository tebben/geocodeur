package database

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

func CreateDB(connectionString string) {
	pool, err := GetDBPool("geocodeur", connectionString)
	if err != nil {
		log.Fatalf("Failed to get database pool: %v", err)
	}

	err = configureDatabase(pool)
	if err != nil {
		log.Fatalf("Failed to configure database: %v", err)
	}

	err = createTableOverture(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	err = createTableSearch(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	processParquet(pool, "../data/download/geocodeur_division.geoparquet")
	processParquet(pool, "../data/download/geocodeur_segment.geoparquet")
	processParquet(pool, "../data/download/geocodeur_water.geoparquet")
	processParquet(pool, "../data/download/geocodeur_poi.geoparquet")

	fmt.Println("Reindexing tables")
	err = reindex(pool)
	if err != nil {
		log.Fatalf("Failed to reindex table: %v", err)
	}

	fmt.Println("Running full vacuum")
	err = vacuum(pool)
	if err != nil {
		log.Fatalf("Failed to vacuum table: %v", err)
	}
}

func processParquet(pool *pgxpool.Pool, path string) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}

	pr, err := reader.NewParquetReader(fr, new(Record), 4)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}

	numRows := int(pr.GetNumRows())
	records := make([]Record, numRows)

	if err := pr.Read(&records); err != nil {
		log.Fatalf("Failed to read records: %v", err)
	}

	pr.ReadStop()
	fr.Close()

	var wg sync.WaitGroup
	batchSize := 100
	numBatches := (numRows + batchSize - 1) / batchSize

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > numRows {
			end = numRows
		}

		wg.Add(1)
		go func(records []Record) {
			defer wg.Done()
			if err != nil {
				log.Fatalf("Failed to begin transaction: %v", err)
			}

			tx, err := pool.Begin(context.Background())
			if err != nil {
				log.Fatalf("Failed to begin transaction: %v", err)
			}
			defer tx.Rollback(context.Background())

			for _, rec := range records {
				id := getNextID()
				err := addOvertureFeature(tx, rec, id)

				if err != nil {
					log.Printf("Failed to insert record: %v", err)
				}

				// Insert alias for name + relation
				if len(rec.Relation) > 0 {
					relations := strings.Split(rec.Relation, ";")
					for _, relation := range relations {
						if rec.Name == relation {
							continue
						}

						alias := rec.Name + " " + relation
						addAlias(tx, rec.ID, alias, id)

						// Add entry for relation aliases
						for name, alias := range aliases {
							if relation == name {
								aliasEmbedding := rec.Name + " " + alias
								addAlias(tx, rec.ID, aliasEmbedding, id)
							}
						}
					}
				}

				// Add name as alias
				addAlias(tx, rec.ID, rec.Name, id)

				// Add aliases for name aliases
				for name, alias := range aliases {
					if rec.Name == name {
						addAlias(tx, rec.ID, alias, id)
					}
				}

				// Add embedding for truncated names
				for _, truncation := range truncations {
					if strings.Contains(rec.Name, truncation) {
						alias := strings.Trim(strings.Replace(rec.Name, truncation, "", 1), " ")
						addAlias(tx, rec.ID, alias, id)
					}
				}

			}

			if err := tx.Commit(context.Background()); err != nil {
				log.Fatalf("Failed to commit transaction: %v", err)
			}

		}(records[start:end])
	}

	wg.Wait()

	fmt.Printf("Inserted %s\n", path)
}

func addOvertureFeature(tx pgx.Tx, rec Record, recordId uint64) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, name, class, subclass, divisions, geom) VALUES ($1, $2, $3, $4, string_to_array($5, ';'), ST_GeomFromText($6, 4326));`, TABLE_OVERTURE)
	_, err := tx.Exec(context.Background(), query, recordId, rec.Name, rec.Class, rec.Subclass, rec.Relation, rec.Geom)

	return err
}

func addAlias(tx pgx.Tx, id, alias string, recordId uint64) error {
	alias = strings.ToLower(alias)
	query := fmt.Sprintf(`INSERT INTO %s (feature_id, alias) VALUES ($1, $2)`, TABLE_SEARCH)
	_, err := tx.Exec(context.Background(), query, recordId, alias)

	return err
}

func reindex(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		REINDEX TABLE %s;
		REINDEX TABLE %s;
	`, TABLE_OVERTURE, TABLE_SEARCH)

	_, err := pool.Exec(context.Background(),
		query)
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

func configureDatabase(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), "ALTER SYSTEM SET work_mem = '256MB';")
	return err
}

func createTableOverture(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE EXTENSION IF NOT EXISTS postgis;

		DROP TABLE IF EXISTS %[1]s CASCADE;

		CREATE TABLE %[1]s (
			id BIGINT PRIMARY KEY,
			name TEXT,
			class TEXT,
			subclass TEXT,
			divisions TEXT[],
			geom geometry(Geometry, 4326)
		);
	`, TABLE_OVERTURE)

	_, err := pool.Exec(context.Background(), query)
	return err
}

// Recreate the table in PostgreSQL
func createTableSearch(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
        CREATE EXTENSION IF NOT EXISTS pg_trgm;

        DROP TABLE IF EXISTS %[1]s;
        DROP INDEX IF EXISTS idx_%[1]s_trgm;
		DROP INDEX IF EXISTS idx_%[1]s_fts;

        CREATE TABLE %[1]s (
			feature_id BIGINT,
            alias TEXT
        );

		-- index for trgm search, gin > gist for our case, gist can be very slow
		CREATE INDEX IF NOT EXISTS idx_%[1]s_trgm ON %[1]s USING gin (alias gin_trgm_ops);

		-- index for FTS search
		CREATE INDEX idx_%[1]s_fts ON public.overture_search USING GIN (to_tsvector('simple', alias));

		ALTER TABLE %[1]s ADD CONSTRAINT fk_%[1]s_feature_id FOREIGN KEY (feature_id) REFERENCES %[2]s (id) ON DELETE CASCADE;
    `, TABLE_SEARCH, TABLE_OVERTURE)

	_, err := pool.Exec(context.Background(), query)
	return err
}
