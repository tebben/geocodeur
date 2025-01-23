package preprocess

import (
	"database/sql"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	log "github.com/sirupsen/logrus"
)

func ProcessAll() {
	process("division", DivisionQuery)
	process("road", RoadQuery)
	process("water", WaterQuery)
	process("poi", PoiQuery)
}

func process(name string, query string) {
	log.Infof("Processing data: %s", name)

	query = strings.ReplaceAll(query, "%DATADIR%", "../data/download/")

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
