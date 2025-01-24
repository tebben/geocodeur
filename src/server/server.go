package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/tebben/geocodeur/api/handlers"
	"github.com/tebben/geocodeur/api/middleware"
	"github.com/tebben/geocodeur/database"
	"github.com/tebben/geocodeur/settings"
)

// Start starts the PGRest server with the given configuration.
// It initializes the necessary resources, sets up the main handler,
// and listens for incoming HTTP requests on the specified port.
func Start(config settings.Config) {
	router := createRouter(config)
	server := &http.Server{Addr: fmt.Sprintf(":%v", config.Server.Port), Handler: router}
	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sig

		log.Info("Stop signal received, shutting down server...")

		shutdownCtx, cancel := context.WithTimeout(serverCtx, 5*time.Second)
		defer cancel()

		go func() {
			<-shutdownCtx.Done()
			if shutdownCtx.Err() == context.DeadlineExceeded {
				log.Fatal("graceful shutdown timed out.. forcing exit.")
			}
		}()

		err := server.Shutdown(shutdownCtx)
		if err != nil {
			log.Fatal(err)
		}

		log.Info("Server stopped successfully")
		serverStopCtx()
	}()

	log.Info(fmt.Sprintf("PGRest started, running on port %v", config.Server.Port))
	defer database.CloseDBPools()

	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	// Wait for server context to be stopped
	<-serverCtx.Done()
}

// createRouter creates and configures the router for the server.
// It sets up the necessary middleware and routes for handling API requests.
// The router is configured with the provided `config` settings.
func createRouter(config settings.Config) http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.Logger("router", log.StandardLogger(), logrus.DebugLevel))
	router.Use(chimiddleware.Recoverer)
	router.Use(chimiddleware.Throttle(config.Server.MaxConcurrentRequests))
	router.Use(chimiddleware.Timeout(time.Duration(config.Server.Timeout) * time.Second))

	router.NotFound(handlers.NotFoundHandler)
	router.Route("/api/geocode", func(r chi.Router) {
		r.Use(middleware.CORSMiddleware(config.Server.CORS))
		r.Get("/", handlers.GeocodeHandler(config))
	})

	startTime := time.Now()
	router.Route("/api/status", func(r chi.Router) {
		r.Use(middleware.CORSMiddleware(config.Server.CORS))
		r.Use(chimiddleware.NoCache)
		r.Get("/", handlers.StatusHandler(startTime))
	})

	return router
}

func TimeoutHandler(timeout time.Duration) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, timeout, "Timeout.")
	}
}
