package service

import (
	"context"
	"fmt"

	"github.com/tebben/geocodeur/database"
)

type GeocodeResult struct {
	Name       string
	Class      string
	Subclass   string
	Alias      string
	Similarity float64
}

func Geocode(connectionString string, pgtrgmTreshold float64, input string) ([]GeocodeResult, error) {
	pool, err := database.GetDBPool("geocodeur", connectionString)
	if err != nil {
		return nil, err
	}

	// Speed up queries by setting the similarity threshold, default is 0.3
	if pgtrgmTreshold != 0.3 {
		pool.Exec(context.Background(), fmt.Sprintf("SET pg_trgm.similarity_threshold = %v;", pgtrgmTreshold))
	}

	// Tested naive query:
	// fetch 100+ rows or more and then merging with same id and then sorting by similarity
	// then taking top 10, this does not improve performance that much and does not respect
	// the limit if many same ids are present for the query
	query := fmt.Sprintf(`
	WITH ranked_aliases AS (
			SELECT
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
		SELECT name, class, subclass, alias, sim
		FROM ranked_aliases
		WHERE rnk = 1
		ORDER BY sim desc
		LIMIT 10;`, database.TABLE_OVERTURE, database.TABLE_SEARCH)

	rows, err := pool.Query(context.Background(), query, input)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []GeocodeResult
	for rows.Next() {
		var name, class, subclass, alias string
		var sim float64
		if err := rows.Scan(&name, &class, &subclass, &alias, &sim); err != nil {
			return nil, err
		}

		results = append(results, GeocodeResult{name, class, subclass, alias, sim})
	}

	return results, nil
}
