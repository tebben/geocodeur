package utils

import (
	"io"
	"net/http"
	"strings"
)

// Contains checks if a given string is present in a slice of strings.
// It returns true if the string is found, otherwise false.
func Contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// getBody reads the body of an HTTP request and returns it as a string.
func GetBodyString(r *http.Request) string {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ""
	}

	// Reset the request body to the original state
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	return string(body)
}
