package settings

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var config Config
var configFile = getConfigLocation()

type Config struct {
	Server   ServerConfig   `json:"server"`
	API      APIConfig      `json:"api"`
	Database DatabaseConfig `json:"database"`
	Process  ProcessConfig  `json:"process"`
}

type ServerConfig struct {
	Port                  int        `json:"port"`
	Debug                 bool       `json:"debug"`
	CORS                  CorsConfig `json:"cors"`
	MaxConcurrentRequests int        `json:"maxConcurrentRequests"`
	Timeout               int        `json:"timeout"`
}

type APIConfig struct {
	PGTRGMTreshold float64 `json:"similarityThreshold"`
}

type DatabaseConfig struct {
	Name             string `json:"name"`
	Schema           string `json:"schema"`
	Tablespace       string `json:"tablespace"`
	ConnectionString string `json:"connectionString"`
	MaxConnections   int32  `json:"maxConnections"`
}

type CorsConfig struct {
	AllowOrigins []string `json:"allowOrigins"`
	AllowHeaders []string `json:"allowHeaders"`
	AllowMethods []string `json:"allowMethods"`
}

type ProcessConfig struct {
	Folder      string `json:"folder"`
	CountryClip string `json:"countryClip"`
}

// getConfigLocation returns the location of the Geocodeur configuration file.
// If the environment variable GEOCODEUR_CONFIG_PATH is set, it returns its value.
// Otherwise, it returns the default location "./config/geocodeur.conf".
func getConfigLocation() string {
	location := os.Getenv("GEOCODEUR_CONFIG_PATH")
	if location == "" {
		location = "../config/geocodeur.conf"
	}
	return location
}

// InitializeConfig loads the configuration
// returns an error if there was a problem loading the configuration.
func InitializeConfig() error {
	err := loadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	return nil
}

// loadConfig loads the configuration from a JSON file.
// It reads the JSON file, unmarshals it into the 'config' variable,
// and sets default values if necessary.
// Returns an error if there was a problem reading or unmarshaling the JSON file.
// loadConfig loads the configuration from a JSON file.
// It reads the JSON file, unmarshals it into the 'config' variable,
// and sets default values if necessary.
// Returns an error if there was a problem reading or unmarshaling the JSON file.
func loadConfig() error {
	jsonFile, err := os.Open(configFile)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return err
	}

	// Preprocess the JSON to remove excessive commas
	cleanedJSON := cleanJSON(string(byteValue))

	err = json.Unmarshal([]byte(cleanedJSON), &config)
	if err != nil {
		return err
	}

	if config.Server.Port == 0 {
		config.Server.Port = 8080
	}

	if config.Server.MaxConcurrentRequests == 0 {
		config.Server.MaxConcurrentRequests = 15
	}

	if config.Server.Timeout == 0 {
		config.Server.Timeout = 30
	}

	// if debug is not set, default to false
	if !config.Server.Debug {
		config.Server.Debug = false
	}

	if len(config.Server.CORS.AllowOrigins) == 0 {
		config.Server.CORS.AllowOrigins = []string{"*"}
	}

	if len(config.Server.CORS.AllowHeaders) == 0 {
		config.Server.CORS.AllowHeaders = []string{"*"}
	}

	if len(config.Server.CORS.AllowMethods) == 0 {
		config.Server.CORS.AllowMethods = []string{"GET", "OPTIONS"}
	}

	if config.API.PGTRGMTreshold == 0 {
		config.API.PGTRGMTreshold = 0.45
	}

	if config.Database.Name == "" {
		config.Database.Name = "geocodeur"
	}

	if config.Database.MaxConnections == 0 {
		config.Database.MaxConnections = 5
	}

	return nil
}

func cleanJSON(input string) string {
	// Remove trailing commas before closing braces and brackets
	re := regexp.MustCompile(`,\s*([\]}])`)
	cleaned := re.ReplaceAllString(input, "$1")
	// Ensure that there are no consecutive commas
	cleaned = strings.ReplaceAll(cleaned, ",,", ",")
	return cleaned
}

// GetConfig returns the current configuration.
func GetConfig() Config {
	return config
}
