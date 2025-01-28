package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

		geocodeOptions, apiError := createGeocoderOptions(config, r)
		if apiError != nil {
			HandleError(w, apiError)
			return
		}

		timeStart := time.Now()
		results, err := service.Geocode(config.Database.ConnectionString, geocodeOptions, query)
		if err != nil {
			apiError := errors.NewAPIError(http.StatusBadRequest, fmt.Sprintf("Error: %v", err), nil)
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

func createGeocoderOptions(config settings.Config, r *http.Request) (service.GeocodeOptions, *errors.APIError) {
	limit, err := getLimit(r)
	if err != nil {
		return service.GeocodeOptions{}, errors.NewAPIError(http.StatusBadRequest, err.Error(), nil)
	}

	classes, err := getClasses(r)
	if err != nil {
		return service.GeocodeOptions{}, errors.NewAPIError(http.StatusBadRequest, err.Error(), nil)
	}

	return service.NewGeocodeOptions(config.API.PGTRGMTreshold, limit, classes), nil
}

func getClasses(r *http.Request) ([]service.Class, error) {
	qClass := r.URL.Query().Get("class")

	if qClass != "" {
		splitClasses := strings.Split(qClass, ",")
		classes := make([]service.Class, len(splitClasses))
		for i, v := range splitClasses {
			class, err := service.StringToClass(strings.ToLower(v))
			if err != nil {
				return nil, err
			}

			classes[i] = class
		}

		return classes, nil
	}

	return nil, nil
}

func getLimit(r *http.Request) (uint16, error) {
	qLimit := r.URL.Query().Get("limit")

	if qLimit != "" {
		limit, err := strconv.ParseUint(qLimit, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Invalid limit: %s", qLimit)
		}

		if limit > 100 {
			return 0, fmt.Errorf("Limit exceeds maximum of 100")
		}

		return uint16(limit), nil
	}

	return 10, nil
}
