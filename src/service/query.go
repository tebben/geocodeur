package service

import (
	"context"
	"fmt"
	"math"

	"github.com/tebben/geocodeur/database"
)

type GeocodeResult struct {
	Name       string  `json:"name"`
	Class      string  `json:"class"`
	Subclass   string  `json:"subclass"`
	Divisions  string  `json:"divisions"`
	Alias      string  `json:"alias"`
	Similarity float64 `json:"similarity"`
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

	query := fmt.Sprintf(`
	WITH ranked_aliases AS (
			SELECT
				a.name,
				a.class,
				a.subclass,
				array_to_string(a.divisions, ',') AS divisions,
				b.alias,
				similarity(b.alias, $1) AS sim,
				CASE
					WHEN a.class = 'division' THEN 1
					WHEN a.class = 'road' THEN 2
					WHEN a.class = 'poi' THEN 3
					ELSE 100
				END AS class_score,
				CASE
					WHEN a.subclass = 'locality' THEN 1
					WHEN a.subclass = 'county' THEN 2
					WHEN a.subclass = 'neighboorhood' THEN 3
					WHEN a.subclass = 'microhood' THEN 4
					-- roads up to living_street the rest gets a high score
					WHEN a.subclass = 'motorway' THEN 1
					WHEN a.subclass = 'trunk' THEN 2
					WHEN a.subclass = 'primary' THEN 3
					WHEN a.subclass = 'secondary' THEN 4
					WHEN a.subclass = 'tertiary' THEN 5
					WHEN a.subclass = 'unclassified' THEN 6
					WHEN a.subclass = 'residential' THEN 7
					WHEN a.subclass = 'living_street' THEN 8
					ELSE 100
				END AS subclass_score,
				ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY similarity(b.alias, $1) DESC) AS rnk
			FROM %s a
			JOIN %s b ON a.id = b.id
			WHERE b.alias %% $1
		)
		SELECT name, class, subclass, divisions, alias, sim
		FROM ranked_aliases
		WHERE rnk = 1
		ORDER BY sim desc, class_score asc, subclass_score asc
		LIMIT 10;`, database.TABLE_OVERTURE, database.TABLE_SEARCH)

	rows, err := pool.Query(context.Background(), query, input)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []GeocodeResult
	for rows.Next() {
		var name, class, subclass, divisions, alias string
		var sim float64
		if err := rows.Scan(&name, &class, &subclass, &divisions, &alias, &sim); err != nil {
			return nil, err
		}

		sim = math.Round(sim*1000) / 1000

		results = append(results, GeocodeResult{name, class, subclass, divisions, alias, sim})
	}

	return results, nil
}
