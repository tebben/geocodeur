package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/anush008/fastembed-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

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
	os.Setenv("ONNX_PATH", "/home/time/.local/lib/python3.11/site-packages/onnxruntime/capi/libonnxruntime.so.1.20.1")
	//createDB()
	queryDB()
}

func queryDB() {
	//query := "pettelaarpark 's-Hertogenbosch"
	query := "van veldekekade"
	model := getEmbeddingModel()
	defer model.Destroy()

	vector := createEmbedding(model, query)
	vectorString := vectorToString(vector)

	// example query: SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
	pool := getDBPool()
	defer pool.Close()
	/*
		-- We have multiple embeddings for each overture record, we only want the closest one
		-- this goes from 6ms to 15ms
		WITH ranked_embeddings AS (
			SELECT
				o.id, o.name, o.class, o.subclass, vector,
				vector <-> $1 AS distance,
				ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY vector <-> $1) AS rn
			FROM overture_embeddings e
			LEFT JOIN overture as o ON o.id = e.id
		)
		SELECT id, name, class, subclass, vector, distance
		FROM ranked_embeddings
		WHERE rn = 1
		ORDER BY distance
		LIMIT 5


		SELECT o.id, o.name, o.class, o.subclass, vector, vector <-> $1 AS distance
		FROM overture_embeddings e
		LEFT JOIN overture as o ON o.id = e.id
		ORDER BY vector <-> $1
		LIMIT 5
	*/

	timeStart := time.Now()
	rows, err := pool.Query(context.Background(), `
		SELECT o.id, o.name, o.class, o.subclass, vector, vector <-> $1 AS distance
		FROM overture_embeddings e
		LEFT JOIN overture as o ON o.id = e.id
		ORDER BY vector <-> $1
		LIMIT 20
	`, vectorString)
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}
	defer rows.Close()
	timeEnd := time.Now()

	fmt.Printf("Query result for: %s\n", query)

	for rows.Next() {
		var id, name, class, subclass, vector string
		var distance float64
		if err := rows.Scan(&id, &name, &class, &subclass, &vector, &distance); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		log.Printf("ID: %s, Name: %s, Class: %s, Subclass: %s, Distance: %f",
			id, name, class, subclass, distance)

	}

	log.Printf("Query took %v", timeEnd.Sub(timeStart))

}

func createDB() {
	processParquet("./data/download/geocodeur_division.geoparquet")
	processParquet("./data/download/geocodeur_segment.geoparquet")
}

func processParquet(path string) {
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

	// Initialize PostgreSQL connection pool

	pool := getDBPool()
	defer pool.Close()

	// Drop and recreate tables
	err = recreateTable(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	err = recreateTableEmbeddings(pool)
	if err != nil {
		log.Fatalf("Failed to recreate table: %v", err)
	}

	// Prepare insert statements
	/* stmtOverture, err := pool.Prepare(context.Background(), "insert_overture", `
	        INSERT INTO overture (id, name, class, subclass, geom)
	        VALUES ($1, $2, $3, $4, ST_GeomFromText($5, 4326))
	    `)
		if err != nil {
			log.Fatalf("Failed to prepare statement: %v", err)
		}

		stmtEmbedding, err := pool.Prepare(context.Background(), "insert_embedding", `
	        INSERT INTO overture_embeddings (id, vector)
	        VALUES ($1, $2)
	    `)
		if err != nil {
			log.Fatalf("Failed to prepare statement: %v", err)
		}
	*/
	model := getEmbeddingModel()
	defer model.Destroy()

	// Use a wait group to wait for all goroutines to finish
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
			//tx, err := pool.Begin(context.Background())
			if err != nil {
				log.Fatalf("Failed to begin transaction: %v", err)
			}

			for _, rec := range records {
				// Insert overture record in db
				//_, err = tx.Exec(context.Background(), "insert_overture", rec.ID, rec.Name, rec.Class, rec.Subclass, rec.Geom)
				_, err := pool.Exec(context.Background(), `insert into overture (id, name, class, subclass, geom) values ($1, $2, $3, $4, ST_GeomFromText($5, 4326))`, rec.ID, rec.Name, rec.Class, rec.Subclass, rec.Geom)

				if err != nil {
					log.Printf("Failed to insert record (ID: %s): %v", rec.ID, err)
				}

				// Insert embeddings
				if len(rec.Relation) > 0 {
					// split relations by ;
					relations := strings.Split(rec.Relation, ";")
					for _, relation := range relations {
						embedding := rec.Name + " " + relation
						vector := createEmbedding(model, embedding)
						vectorString := vectorToString(vector)

						// Insert into database
						//_, err = tx.Exec(context.Background(), "insert_embedding", rec.ID, vectorString)
						_, err = pool.Exec(context.Background(), `insert into overture_embeddings (id, vector) values ($1, $2)`, rec.ID, vectorString)
						if err != nil {
							log.Printf("Failed to insert record (ID: %s): %v", rec.ID, err)
						}
					}
				}

				embedding := rec.Name
				vector := createEmbedding(model, embedding)
				vectorString := vectorToString(vector)

				// Insert into database
				_, err = pool.Exec(context.Background(), `insert into overture_embeddings (id, vector) values ($1, $2)`, rec.ID, vectorString)
				if err != nil {
					log.Printf("Failed to insert record (ID: %s): %v", rec.ID, err)
				}
			}

			/* if err := tx.Commit(context.Background()); err != nil {
				log.Fatalf("Failed to commit transaction: %v", err)
			} */
		}(records[start:end])
	}

	// Wait for all goroutines to finish
	wg.Wait()
}

func vectorToString(vector []float32) string {
	return fmt.Sprintf("[%s]", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(vector)), ","), "[]"))
}

func getEmbeddingModel() *fastembed.FlagEmbedding {
	options := fastembed.InitOptions{
		Model:     fastembed.AllMiniLML6V2,
		CacheDir:  "model_cache",
		MaxLength: 200,
	}

	model, err := fastembed.NewFlagEmbedding(&options)
	if err != nil {
		log.Fatalf("Failed to initialize fastembed model: %v", err)
	}

	return model
}

func createEmbedding(model *fastembed.FlagEmbedding, data string) []float32 {
	doc := []string{data}
	vector, err := model.Embed(doc, 64)
	if err != nil {
		log.Fatalf("Failed to generate vector: %v", data)
	}

	return vector[0]
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

// Recreate the table in PostgreSQL
func recreateTableEmbeddings(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), `
		CREATE EXTENSION IF NOT EXISTS vector;
		DROP TABLE IF EXISTS overture_embeddings;
		CREATE TABLE overture_embeddings (
			id TEXT,
			vector VECTOR(384)
		);

		--ALTER TABLE overture_embeddings ADD CONSTRAINT fk_overture_id FOREIGN KEY (id) REFERENCES overture(id) ON DELETE CASCADE;
		CREATE INDEX vector_idx ON overture_embeddings USING hnsw (vector vector_l2_ops);
	`)
	return err
}

func recreateTable(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.Background(), `
		CREATE EXTENSION IF NOT EXISTS postgis;
		DROP TABLE IF EXISTS overture;
		CREATE TABLE overture (
			id TEXT,
			name TEXT,
			class TEXT,
			subclass TEXT,
			geom geometry(Geometry, 4326)
		);

		CREATE INDEX idx_overture_id ON overture (id);
	`)
	return err
}
