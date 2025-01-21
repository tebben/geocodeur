package main

import (
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/database"
	"github.com/tebben/geocodeur/preprocess"
	"github.com/tebben/geocodeur/service"
	"github.com/tebben/geocodeur/settings"
)

func main() {
	settings.InitializeConfig()
	config := settings.GetConfig()

	command := os.Args[1]
	if command == "create" {
		database.CreateDB(config.ConnectionString)
	} else if command == "query" {
		query(config)
	} else if command == "process" {
		process()
	} else {
		log.Fatalf("Unknown command")
	}
}

func query(config settings.Config) {
	timeStart := time.Now()
	results, err := service.Geocode(config.ConnectionString, config.PGTRGMTreshold, os.Args[2])
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}
	timeEnd := time.Now()

	for _, result := range results {
		log.Infof("Name: %s, Class: %s, Subclass: %s, Alias: %s, Similarity: %f",
			result.Name, result.Class, result.Subclass, result.Alias, result.Similarity)
	}

	log.Infof("-----------\n")
	log.Infof("%v", timeEnd.Sub(timeStart))
}

func process() {
	preprocess.ProcessAll()
}
