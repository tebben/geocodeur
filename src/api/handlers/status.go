package handlers

import (
	"context"
	"fmt"
	"time"
)

type StatusResult struct {
	Body struct {
		Started string `json:"started" doc:"Time in UTC when the server started"`
		Uptime  string `json:"uptime" doc:"Uptime of geocodeur server"`
	}
}

func StatusHandler(start time.Time) func(ctx context.Context, input *struct{}) (*StatusResult, error) {
	return func(ctx context.Context, input *struct{}) (*StatusResult, error) {
		uptime := formatDuration(time.Since(start))

		statusResult := &StatusResult{}
		statusResult.Body.Started = start.Local().UTC().String()
		statusResult.Body.Uptime = uptime

		return statusResult, nil
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
