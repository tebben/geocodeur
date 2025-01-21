package settings

var config Config

type Config struct {
	ConnectionString string
	PGTRGMTreshold   float64
}

// InitializeConfig loads the configuration
// returns an error if there was a problem loading the configuration.
func InitializeConfig() error {
	err := loadConfig()
	if err != nil {
		return err
	}

	return nil
}

// ToDo: load from json file, currently hardcoded

// loadConfig loads the configuration from a JSON file.
// It reads the JSON file, unmarshals it into the 'config' variable,
// and sets default values if necessary.
// Returns an error if there was a problem reading or unmarshaling the JSON file.
func loadConfig() error {
	config.ConnectionString = "postgres://postgres:postgres@localhost:5432/geocodeur?sslmode=disable"
	config.PGTRGMTreshold = 0.48

	return nil
}

// GetConfig returns the current configuration.
func GetConfig() Config {
	return config
}
