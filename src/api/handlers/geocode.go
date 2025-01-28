package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tebben/geocodeur/errors"
	"github.com/tebben/geocodeur/service"
	"github.com/tebben/geocodeur/settings"
)

type GeocodeResult struct {
	MS      float32                 `json:"ms"`
	Results []service.GeocodeResult `json:"results"`
}

// GeocodeHandler handles the HTTP request for executing a geocode request.
// It takes in the HTTP response writer, the HTTP request and the database connection configuration.
// It returns an error if there was an issue connecting to the database or executing the query.
func GeocodeHandler(config settings.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if query == "" {
			apiError := errors.NewAPIError(http.StatusBadRequest, "Parameter q missing: Query", nil)
			HandleError(w, apiError)
			return
		}

		timeStart := time.Now()

		geocodeOptions := service.NewGeocodeOptions()
		results, err := service.Geocode(config.Database.ConnectionString, geocodeOptions, query)
		if err != nil {
			apiError := errors.NewAPIError(http.StatusBadRequest, fmt.Sprintf("Error", "test"), nil)
			HandleError(w, apiError)
			return
		}
		timeEnd := time.Now()

		geocodeResult := GeocodeResult{
			MS:      float32(timeEnd.Sub(timeStart).Milliseconds()),
			Results: results,
		}

		// Set Content-Type to application/json
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// Encode results to JSON and send as response
		if err := json.NewEncoder(w).Encode(geocodeResult); err != nil {
			http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
		}
	}
}
