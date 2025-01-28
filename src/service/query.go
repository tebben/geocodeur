package service

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/tebben/geocodeur/database"
)

type GeocodeResult struct {
	Name       string  `json:"name"`
	Class      string  `json:"class"`
	Subclass   string  `json:"subclass"`
	Divisions  string  `json:"divisions"`
	Alias      string  `json:"alias"`
	SearchType string  `json:"searchType"`
	Similarity float64 `json:"similarity"`
}

type Class string

const (
	Division Class = "division"
	Road     Class = "road"
	Water    Class = "water"
	Poi      Class = "poi"
)

type GeocodeOptions struct {
	PgtrgmTreshold float64
	Limit          uint16
	Classes        []Class
}

func (g GeocodeOptions) ClassesToSqlArray() string {
	classes := g.Classes
	if len(classes) == 0 {
		classes = []Class{Division, Road, Water, Poi}
	}

	lowerClasses := make([]string, len(classes))
	for i, class := range classes {
		lowerClasses[i] = fmt.Sprintf("'%s'", strings.ToLower(string(class)))
	}

	return fmt.Sprintf("(%s)", strings.Join(lowerClasses, ", "))
}

// new GeocodeOptions with default values
func NewGeocodeOptions() GeocodeOptions {
	return GeocodeOptions{
		PgtrgmTreshold: 0.3,
		Limit:          10,
		Classes:        []Class{Division, Road, Water, Poi},
	}
}

func Geocode(connectionString string, options GeocodeOptions, input string) ([]GeocodeResult, error) {
	pool, err := database.GetDBPool("geocodeur", connectionString)
	if err != nil {
		return nil, err
	}

	input = strings.ToLower(input)

	// Speed up queries by setting the similarity threshold, default is 0.3
	if options.PgtrgmTreshold != 0.3 {
		pool.Exec(context.Background(), fmt.Sprintf("SET pg_trgm.similarity_threshold = %v;", options.PgtrgmTreshold))
	}

	classesIn := options.ClassesToSqlArray()

	query := fmt.Sprintf(`
	WITH fts AS (
		SELECT feature_id, alias, similarity(alias, $1) AS sim, 'fts' as search
		FROM %s AS a
		WHERE to_tsvector('simple', a.alias) @@ to_tsquery('simple',
			replace($1, ' ', ':* & ') || ':*'
		)
		ORDER BY sim
	),
	trgm AS (
		SELECT feature_id, alias, similarity(a.alias, $1) AS sim, 'trgm' as search
		FROM %s AS a
		WHERE a.alias %% $1
		ORDER BY a.alias <-> $1
	),
	alias_results AS (
		SELECT *
		FROM fts
		UNION ALL
		SELECT *
		FROM trgm
		WHERE NOT EXISTS (SELECT 1 FROM fts)
	), ranked_aliases AS (
		SELECT
			a.id,
			a.name,
			a.class,
			a.subclass,
			array_to_string(a.divisions, ',') AS divisions,
			b.alias,
			a.geom,
			b.sim,
			b.search,
			CASE
				WHEN a.class = 'division' THEN 1
				WHEN a.class = 'water' THEN 2
				WHEN a.class = 'road' THEN 3
				WHEN a.class = 'poi' THEN 4
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
		JOIN alias_results b ON a.id = b.feature_id
	)
	SELECT name, class, subclass, divisions, alias, search, sim
	FROM ranked_aliases
	WHERE rnk = 1
	AND class IN %s
	ORDER BY sim desc, class_score asc, subclass_score asc
	LIMIT %v;`, database.TABLE_SEARCH, database.TABLE_SEARCH, database.TABLE_OVERTURE, classesIn, options.Limit)

	rows, err := pool.Query(context.Background(), query, input)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []GeocodeResult
	for rows.Next() {
		var name, class, subclass, divisions, alias, search string
		var sim float64
		if err := rows.Scan(&name, &class, &subclass, &divisions, &alias, &search, &sim); err != nil {
			return nil, err
		}

		sim = math.Round(sim*1000) / 1000
		results = append(results, GeocodeResult{name, class, subclass, divisions, alias, search, sim})
	}

	return results, nil
}
