package main

import (
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/database"
	"github.com/tebben/geocodeur/preprocess"
	"github.com/tebben/geocodeur/server"
	"github.com/tebben/geocodeur/service"
	"github.com/tebben/geocodeur/settings"
)

func initLogger(config settings.Config) {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	if config.Server.Debug {
		log.SetLevel(log.DebugLevel)
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
}

func main() {
	log.Info("Starting geocodeur")
	if len(os.Args) < 1 {
		log.Fatal("No command provided")
	}

	err := settings.InitializeConfig()
	if err != nil {
		log.Fatal(err)
	}

	config := settings.GetConfig()
	initLogger(config)

	command := os.Args[1]
	if command == "create" {
		database.CreateDB(config)
	} else if command == "query" {
		query(config)
	} else if command == "process" {
		process()
	} else if command == "server" {
		server.Start(config)
	} else {
		log.Fatalf("Unknown command")
	}
}

func query(config settings.Config) {
	timeStart := time.Now()
	geocodeOptions := service.NewGeocodeOptions(config.API.PGTRGMTreshold, 10, nil)
	results, err := service.Geocode(config.Database.ConnectionString, geocodeOptions, os.Args[2])
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}
	timeEnd := time.Now()

	for _, result := range results {
		log.Infof("Name: %s, Class: %s, Subclass: %s, Alias: %s, Similarity: %f, Search: %s",
			result.Name, result.Class, result.Subclass, result.Alias, result.Similarity, result.SearchType)
	}

	log.Infof("-----------\n")
	log.Infof("%v", timeEnd.Sub(timeStart))
}

func process() {
	preprocess.ProcessAll()
}
