package service

import "encoding/json"

type LookupResult struct {
	ID        uint64          `json:"id" doc:"The id of the feature, not the original Overture id"`
	Name      string          `json:"name" doc:"The name of the feature"`
	Class     string          `json:"class" doc:"The class of the feature"`
	Subclass  string          `json:"subclass" doc:"The subclass of the feature"`
	Divisions string          `json:"divisions" doc:"The divisions of the feature"`
	Geom      json.RawMessage `json:"geom" doc:"The geometry of the feature in GeoJSON format"`
}

/*
curl \
  -X GET 'http://localhost:7700/indexes/geocodeur/documents/1320034' \
  -H 'Authorization: Bearer E8H-DDQUGhZhFWhTq263Ohd80UErhFmLIFnlQK81oeQ' \
  -H 'Content-Type: application/json'

  curl \
  -X GET 'http://localhost:7700/indexes/geocodeur/settings/ranking-rules' \
   -H 'Authorization: Bearer E8H-DDQUGhZhFWhTq263Ohd80UErhFmLIFnlQK81oeQ' \
   -H 'Content-Type: application/json'

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/database"
	"github.com/tebben/geocodeur/settings"
)

type LookupResult struct {
	ID        uint64          `json:"id" doc:"The id of the feature, not the original Overture id"`
	Name      string          `json:"name" doc:"The name of the feature"`
	Class     string          `json:"class" doc:"The class of the feature"`
	Subclass  string          `json:"subclass" doc:"The subclass of the feature"`
	Divisions string          `json:"divisions" doc:"The divisions of the feature"`
	Geom      json.RawMessage `json:"geom" doc:"The geometry of the feature in GeoJSON format"`
}

func Lookup(connectionString string, id uint64) (LookupResult, error) {
	config := settings.GetConfig()
	pool, err := database.GetDBPool("geocodeur", config.Database)
	if err != nil {
		log.Errorf("Error getting database pool: %v", err)
		return LookupResult{}, fmt.Errorf("Error connecting to database")
	}

	// Construct the query
	query := createLookupQuery()

	// Execute the query
	row := pool.QueryRow(context.Background(), query, id)

	// Parse the results
	result, err := parseLookupResults(row)
	if err != nil {
		return LookupResult{}, err
	}

	return result, nil
}

func parseLookupResults(row pgx.Row) (LookupResult, error) {
	var name, class, subclass, divisions string
	var id uint64
	var geom sql.NullString

	if err := row.Scan(&id, &name, &class, &subclass, &divisions, &geom); err != nil {
		return LookupResult{}, err
	}

	result := LookupResult{
		ID:        id,
		Name:      name,
		Class:     class,
		Subclass:  subclass,
		Divisions: divisions,
		Geom:      json.RawMessage(geom.String),
	}

	return result, nil
}

func createLookupQuery() string {
	return fmt.Sprintf(`
			SELECT
				id,
				name,
				class,
				subclass,
				array_to_string(divisions, ',') AS divisions,
				ST_AsGeoJSON(geom) AS geom
			FROM
				%s
			WHERE
				id = $1;`,
		database.TABLE_OVERTURE)
}
*/
