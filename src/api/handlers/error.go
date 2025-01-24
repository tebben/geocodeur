package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tebben/geocodeur/errors"

	log "github.com/sirupsen/logrus"
)

// HandleError handles the error and sends an appropriate response to the client.
// If the error is of type *APIError, it sets the response status code and encodes the error as JSON.
// For unexpected errors, it sets the response status code to 500 and creates a new APIError with a generic message.
func HandleError(w http.ResponseWriter, err error) {
	if err != nil {
		if apiErr, ok := err.(*errors.APIError); ok {
			if apiErr.Details != nil {
				log.Errorf("Error handling request: %v", fmt.Sprintf("error: %v, details: %v", apiErr, *apiErr.Details))
			} else {
				log.Errorf("Error handling request: %v", fmt.Sprintf("error: %v", apiErr))
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(apiErr.StatusCode)
			json.NewEncoder(w).Encode(apiErr)
			return
		}

		// Fallback for unexpected errors
		w.WriteHeader(http.StatusInternalServerError)
		response := errors.NewAPIError(http.StatusInternalServerError, "An unexpected error occurred", nil)
		json.NewEncoder(w).Encode(response)
	}
}
