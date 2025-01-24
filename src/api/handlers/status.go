package handlers

import (
	"fmt"
	"net/http"
	"time"
)

// StatusHandler handles the status check endpoint.
// It sets the Content-Type header to "application/json" and returns a 200 OK status code.
func StatusHandler(start time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		uptime := formatDuration(time.Since(start))
		_, _ = w.Write([]byte(`{"status": "ok", "started": "` + start.Local().UTC().String() + `", "uptime": "` + uptime + `"}`))
	}
}

// formatDuration formats a time.Duration into a more readable string.
func formatDuration(d time.Duration) string {
	seconds := int(d.Seconds())
	days := seconds / 86400
	seconds -= days * 86400
	hours := seconds / 3600
	seconds -= hours * 3600
	minutes := seconds / 60
	seconds -= minutes * 60

	if days > 0 {
		return fmt.Sprintf("%dd %02dh %02dm %02ds", days, hours, minutes, seconds)
	} else if hours > 0 {
		return fmt.Sprintf("%02dh %02dm %02ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%02dm %02ds", minutes, seconds)
	}
	return fmt.Sprintf("%02ds", seconds)
}
