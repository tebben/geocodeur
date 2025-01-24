package middleware

import (
	"net/http"

	"github.com/tebben/geocodeur/settings"
)

// CORSMiddleware is a middleware function that adds Cross-Origin Resource Sharing (CORS) headers to the HTTP response.
// It takes a `corsConfig` parameter of type `settings.CorsConfig` which contains the CORS configuration settings.
// The function returns a new middleware function that can be used with `http.Handler` instances.
// The returned middleware function adds the necessary CORS headers to the response and handles preflight OPTIONS requests.
// It then calls the next handler in the chain.
func CORSMiddleware(corsConfig settings.CorsConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", corsConfig.GetAllowOriginsString())
			w.Header().Set("Access-Control-Allow-Methods", corsConfig.GetAllowMethodsString())
			w.Header().Set("Access-Control-Allow-Headers", corsConfig.GetAllowHeadersString())

			// Handle preflight OPTIONS requests
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			// Call the next handler
			next.ServeHTTP(w, r)
		})
	}
}
