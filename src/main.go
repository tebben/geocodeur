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
	err := settings.InitializeConfig()
	if err != nil {
		log.Fatalf("Failed to initialize configuration: %v", err)
	}

	config := settings.GetConfig()
	initLogger(config)

	command := os.Args[1]
	if command == "create" {
		database.CreateDB(config.Database.ConnectionString)
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
	results, err := service.Geocode(config.Database.ConnectionString, config.API.PGTRGMTreshold, os.Args[2])
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
