package errors

import "net/http"

// APIError represents an error returned by the API.
type APIError struct {
	StatusCode int     `json:"status"`            // The HTTP status code of the error.
	StatusText string  `json:"statusText"`        // The HTTP status text of the error.
	Message    string  `json:"error"`             // The error message.
	Details    *string `json:"details,omitempty"` // Additional details about the error (optional).
}

// Implement the Error method to satisfy the error interface
func (e *APIError) Error() string {
	return e.Message
}

// NewAPIError creates a new APIError with the given status code, test, message, and details
func NewAPIError(statusCode int, message string, details *string) *APIError {
	if message == "" {
		message = http.StatusText(statusCode)
	}

	return &APIError{
		StatusCode: statusCode,
		StatusText: http.StatusText(statusCode),
		Message:    message,
		Details:    details,
	}
}
