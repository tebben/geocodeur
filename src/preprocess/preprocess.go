package preprocess

import (
	"database/sql"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/preprocess/queries"
	"github.com/tebben/geocodeur/settings"
)

func ProcessAll() {
	process("division", queries.DivisionQuery)
	process("road", queries.RoadQuery)
	process("water", queries.WaterQuery)
	process("poi", queries.PoiQuery)
	process("infra", queries.InfraQuery)
}

func process(name string, query string) {
	log.Infof("Processing data: %s", name)

	config := settings.GetConfig()
	query = strings.ReplaceAll(query, "%DATADIR%", config.Process.Folder)
	query = strings.ReplaceAll(query, "%COUNTRY%", strings.ToLower(config.Process.CountryClip))

	db, err := getDuckDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(query)
	if err != nil {
		panic(err)
	}
}

func getDuckDB() (*sql.DB, error) {
	return sql.Open("duckdb", "")
}
