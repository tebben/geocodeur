package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var aliases = map[string]string{
	"'s-Hertogenbosch": "Den Bosch",
}

var truncations = []string{
	"Rijksweg",
}

var TABLEOVERTURE = "overture"
var TABLESEARCH = "overture_search"

// Define your structure matching the Parquet schema
type Record struct {
	ID       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Geom     string `parquet:"name=geom, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Class    string `parquet:"name=class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Subclass string `parquet:"name=subclass, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
	Relation string `parquet:"name=relation, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`
}

func main() {
	command := os.Args[1]
	if command == "create" {
		createDB()
	} else if command == "query" {
		queryDB(os.Args[2])
	} else {
		log.Fatalf("Unknown command")
	}
}

func queryDB(input string) {
	pool := getDBPool()
	defer pool.Close()

	timeStart := time.Now()

	query := fmt.Sprintf(`
	WITH ranked_aliases AS (
			SELECT
				a.id,
				a.name,
				a.class,
				a.subclass,
				b.alias,
				similarity(b.alias, $1) AS sim,
				ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY similarity(b.alias, $1) DESC) AS rnk
			FROM %s a
			JOIN %s b ON a.id = b.id
			WHERE b.alias %% $1
		)
		SELECT id, name, class, subclass, alias, sim
		FROM ranked_aliases
		WHERE rnk = 1
		ORDER BY sim desc
		LIMIT 10;`, TABLEOVERTURE, TABLESEARCH)

	rows, err := pool.Query(context.Background(), query, input)
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}
	defer rows.Close()
	timeEnd := time.Now()

	fmt.Printf("Query: %s\n", input)

	for rows.Next() {
		var id, name, class, subclass, alias string
		var sim float64
		if err := rows.Scan(&id, &name, &class, &subclass, &alias, &sim); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		log.Printf("ID: %s, Name: %s, Class: %s, Subclass: %s, alias: %s, Similarity: %f",
			id, name, class, subclass, alias, sim)

	}

	log.Printf("-----------\n")
	log.Printf("%v", timeEnd.Sub(timeStart))
}

func createDB() {
	pool := getDBPool()
	defer pool.Close()

	err := createTableOverture(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	err = createTableSearch(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	processParquet(pool, "./data/download/geocodeur_division.geoparquet")
	processParquet(pool, "./data/download/geocodeur_segment.geoparquet")
	processParquet(pool, "./data/download/geocodeur_poi.geoparquet")

	fmt.Println("Reindexing tables")
	err = reindex(pool)
	if err != nil {
		log.Fatalf("Failed to reindex table: %v", err)
	}
}

func processParquet(pool *pgxpool.Pool, path string) {
	// Open GeoParquet file
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
				err := addOvertureFeature(tx, rec)

				if err != nil {
					log.Printf("Failed to insert record: %v", err)
				}

				// Insert alias for name + relation
				if len(rec.Relation) > 0 {
					relations := strings.Split(rec.Relation, ";")
					for _, relation := range relations {
						alias := rec.Name + " " + relation
						addAlias(tx, rec.ID, alias)

						// Add entry for relation aliases
						for name, alias := range aliases {
							if relation == name {
								aliasEmbedding := rec.Name + " " + alias
								addAlias(tx, rec.ID, aliasEmbedding)
							}
						}
					}
				}

				// Add name as alias
				addAlias(tx, rec.ID, rec.Name)

				// Add aliases for name aliases
				for name, alias := range aliases {
					if rec.Name == name {
						addAlias(tx, rec.ID, alias)
					}
				}

				// Add embedding for truncated names
				for _, truncation := range truncations {
					if strings.Contains(rec.Name, truncation) {
						alias := strings.Trim(strings.Replace(rec.Name, truncation, "", 1), " ")
						addAlias(tx, rec.ID, alias)
					}
				}

			}

			if err := tx.Commit(context.Background()); err != nil {
				log.Fatalf("Failed to commit transaction: %v", err)
			}

		}(records[start:end])
	}

	wg.Wait()

	fmt.Printf("Processed %s\n", path)
}

func addOvertureFeature(tx pgx.Tx, rec Record) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, name, class, subclass, geom) VALUES ($1, $2, $3, $4, ST_GeomFromText($5, 4326))`, TABLEOVERTURE)
	_, err := tx.Exec(context.Background(), query, rec.ID, rec.Name, rec.Class, rec.Subclass, rec.Geom)

	return err
}

func addAlias(tx pgx.Tx, id, alias string) error {
	query := fmt.Sprintf(`INSERT INTO %s (id, alias) VALUES ($1, $2)`, TABLESEARCH)
	_, err := tx.Exec(context.Background(), query, id, alias)

	return err
}

func getDBPool() *pgxpool.Pool {
	connStr := "postgres://postgres:postgres@localhost:5432/geocodeur?sslmode=disable"
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return pool
}

func reindex(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		REINDEX TABLE %s;
		REINDEX TABLE %s;
	`, TABLEOVERTURE, TABLESEARCH)

	_, err := pool.Exec(context.Background(),
		query)
	return err
}

func createTableOverture(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
		CREATE EXTENSION IF NOT EXISTS postgis;
		DROP TABLE IF EXISTS %[1]s;

		CREATE TABLE %[1]s (
			id VARCHAR(32) PRIMARY KEY,
			name TEXT,
			class TEXT,
			subclass TEXT,
			geom geometry(Geometry, 4326)
		);
	`, TABLEOVERTURE)

	_, err := pool.Exec(context.Background(), query)
	return err
}

// Recreate the table in PostgreSQL
func createTableSearch(pool *pgxpool.Pool) error {
	query := fmt.Sprintf(`
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        DROP TABLE IF EXISTS %[1]s;
        DROP INDEX IF EXISTS idx_%[1]s_trgm;

        CREATE TABLE %[1]s (
            id VARCHAR(32),
            alias TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_%[1]s_trgm ON %[1]s USING gin (alias gin_trgm_ops);
    `, TABLESEARCH)

	_, err := pool.Exec(context.Background(), query)
	return err
}
