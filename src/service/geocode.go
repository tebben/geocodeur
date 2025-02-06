package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/database"
	"github.com/tebben/geocodeur/settings"
)

type GeocodeResult struct {
	ID         uint64          `json:"id" doc:"The id of the feature, not the original Overture id"`
	Name       string          `json:"name" doc:"The name of the feature"`
	Class      string          `json:"class" doc:"The class of the feature"`
	Subclass   string          `json:"subclass" doc:"The subclass of the feature"`
	Divisions  string          `json:"divisions" doc:"The divisions of the feature"`
	Alias      string          `json:"alias" doc:"The alias of the feature"`
	SearchType string          `json:"searchType" doc:"The search type used to find the result, either fts (Full Text Search) or trgm (Trigram matching/fuzzy search)"`
	Similarity float64         `json:"similarity" doc:"The similarity score q <-> alias, the higher the better"`
	Geom       json.RawMessage `json:"geom,omitempty" doc:"The geometry of the feature in GeoJSON format"`
}

type Class string

const (
	Division Class = "division"
	Road     Class = "road"
	Water    Class = "water"
	Poi      Class = "poi"
	Infra    Class = "infra"
	Address  Class = "address"
	Zipcode  Class = "zipcode"
)

func StringToClass(s string) (Class, error) {
	switch s {
	case string(Division):
		return Division, nil
	case string(Road):
		return Road, nil
	case string(Water):
		return Water, nil
	case string(Poi):
		return Poi, nil
	case string(Infra):
		return Infra, nil
	case string(Address):
		return Address, nil
	case string(Zipcode):
		return Zipcode, nil
	default:
		return "", fmt.Errorf("class %s not found", s)
	}
}

type GeocodeOptions struct {
	PgtrgmTreshold  float64
	Limit           uint16
	Classes         []Class
	IncludeGeometry bool
}

func (g GeocodeOptions) ClassesToSqlArray() string {
	classes := g.Classes
	if classes == nil || len(classes) == 0 {
		classes = []Class{Division, Road, Water, Poi, Infra, Address, Zipcode}
	}

	lowerClasses := make([]string, len(classes))
	for i, class := range classes {
		lowerClasses[i] = fmt.Sprintf("'%s'", strings.ToLower(string(class)))
	}

	return fmt.Sprintf("(%s)", strings.Join(lowerClasses, ", "))
}

// new GeocodeOptions with default values
func NewGeocodeOptions(pgtrmTreshold float64, limit uint16, classes []Class, includeGeom bool) GeocodeOptions {
	return GeocodeOptions{
		PgtrgmTreshold:  pgtrmTreshold,
		Limit:           limit,
		Classes:         classes,
		IncludeGeometry: includeGeom,
	}
}

func Geocode(connectionString string, options GeocodeOptions, input string) ([]GeocodeResult, error) {
	config := settings.GetConfig()
	pool, err := database.GetDBPool("geocodeur", config.Database)
	if err != nil {
		log.Errorf("Error getting database pool: %v", err)
		return nil, fmt.Errorf("Error connecting to database")
	}

	// Everything for search is lower case so we lowercase the input query
	input = strings.ToLower(input)

	// If incoming request has a different pg_trgm similarity threshold than the current one, set it
	if options.PgtrgmTreshold != config.API.PGTRGMTreshold {
		pool.Exec(context.Background(), fmt.Sprintf("SET pg_trgm.similarity_threshold = %v;", options.PgtrgmTreshold))
	}

	// Construct the query
	query := createGeocodeQuery(options, input)

	// Execute the query
	rows, err := pool.Query(context.Background(), query, input)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Parse the results
	results, err := parseGeocodeResults(rows)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func parseGeocodeResults(rows pgx.Rows) ([]GeocodeResult, error) {
	var results []GeocodeResult

	for rows.Next() {
		var name, class, subclass, divisions, alias, search string
		var id uint64
		var sim float64
		var geom sql.NullString // Use NullString to handle cases where geom is excluded

		if err := rows.Scan(&id, &name, &class, &subclass, &divisions, &alias, &search, &sim, &geom); err != nil {
			return nil, err
		}

		sim = math.Round(sim*1000) / 1000
		results = append(results, GeocodeResult{id, name, class, subclass, divisions, alias, search, sim, json.RawMessage(geom.String)})
	}

	return results, nil
}

func createGeocodeQuery(options GeocodeOptions, input string) string {
	classesIn := options.ClassesToSqlArray()

	// workaround for now since we do not have class in the search table
	classesIn = strings.Replace(classesIn, "'division'", "1", -1)
	classesIn = strings.Replace(classesIn, "'water'", "1", -1)
	classesIn = strings.Replace(classesIn, "'road'", "2", -1)
	classesIn = strings.Replace(classesIn, "'infra'", "3", -1)
	classesIn = strings.Replace(classesIn, "'address'", "4", -1)
	classesIn = strings.Replace(classesIn, "'zipcode'", "5", -1)
	classesIn = strings.Replace(classesIn, "'poi'", "6", -1)

	// Conditional geometry column
	geometryColumn := "'' AS geom" // Default to an empty string if geometry is not included
	if options.IncludeGeometry {
		geometryColumn = "ST_AsGeoJSON(b.geom) AS geom"
	}

	return fmt.Sprintf(`
		WITH fts AS (
			SELECT
				feature_id, alias, class_rank, subclass_rank, 'fts' as search
			FROM
				%[1]s
			WHERE
				ABS(word_count - array_length(string_to_array($1, ' '), 1)) < 3
			AND
				ABS(char_count - LENGTH($1)) < 30
			AND
				vector_search @@ to_tsquery('simple', replace($1, ' ', ':* & ') || ':*')
			AND
				class_rank IN %[3]s
			ORDER BY
				class_rank ASC,
				subclass_rank ASC
			LIMIT 100
		),
		trgm AS (
			SELECT feature_id, alias, class_rank, subclass_rank, 'trgm' as search
			FROM %[1]s
			WHERE
				ABS(word_count - array_length(string_to_array($1, ' '), 1)) < 3
			AND
				ABS(char_count - LENGTH($1)) < 30
			AND
				alias %% $1
			AND
				class_rank IN %[3]s
			ORDER BY
				class_rank ASC,
				subclass_rank ASC
			LIMIT 100
		),
		search_results AS (
			SELECT *
			FROM fts
			UNION ALL
			SELECT *
			FROM trgm
			WHERE NOT EXISTS (SELECT 1 FROM fts)
		),
		similarity as (
			select
				feature_id,
				alias,
				class_rank,
				subclass_rank,
				similarity(alias, $1) AS sim,
				search,
				ROW_NUMBER() OVER (PARTITION BY feature_id ORDER BY similarity(alias, $1) DESC) AS rnk
			from search_results
		)
		SELECT
			b.id, b.name, b.class, b.subclass, b.divisions::varchar, a.alias, a.search, a.sim, %[4]s
		FROM similarity AS a
		INNER JOIN
			%[2]s AS b ON a.feature_id = b.id
		WHERE a.rnk = 1
		ORDER by
			sim desc,
			class_rank asc,
			subclass_rank asc
		LIMIT %[5]v;`,
		database.TABLE_SEARCH, database.TABLE_OVERTURE, classesIn, geometryColumn, options.Limit)
}
