package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
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
	router := chi.NewMux()
	router.Use(middleware.Logger("router", log.StandardLogger(), logrus.DebugLevel))
	router.Use(chimiddleware.Recoverer)
	router.Use(chimiddleware.Throttle(config.Server.MaxConcurrentRequests))
	router.Use(chimiddleware.Timeout(time.Duration(config.Server.Timeout) * time.Second))
	router.Use(chimiddleware.Compress(5, "application/json"))
	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   config.Server.CORS.AllowOrigins,
		AllowedMethods:   config.Server.CORS.AllowMethods,
		AllowedHeaders:   config.Server.CORS.AllowHeaders,
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           600,
	}))

	humaConfig := createHumaConfig()
	api := humachi.New(router, humaConfig)
	registerRoutes(api, config)

	return router
}

func createHumaConfig() huma.Config {
	humaConfig := huma.DefaultConfig("Geocodeur", "1.0.0")
	humaConfig.CreateHooks = nil
	humaConfig.Info.Contact = &huma.Contact{
		URL: "https://github.com/tebben/geocodeur",
	}
	humaConfig.Info.Description = "Geocodeur is a RESTful API that provides geocoding and reverse geocoding services based on Overture Maps data. Under the hood, it uses a PostGIS database and uses Full Text Search to provide fast and accurate search, when FTS has no results it falls back to a trigram search based pg_trgm. This way the search is fast and accurate, even when the user makes typing errors."
	humaConfig.Info.License = &huma.License{
		Name: "MIT",
	}

	return humaConfig
}

func registerRoutes(api huma.API, config settings.Config) {
	huma.Register(api, huma.Operation{
		OperationID: "status",
		Method:      http.MethodGet,
		Path:        "/status",
		Summary:     "Status",
		Description: "Get the status of geocodeur.",
	}, handlers.StatusHandler(time.Now()))

	huma.Register(api, huma.Operation{
		OperationID: "geocode",
		Method:      http.MethodGet,
		Path:        "/geocode",
		Summary:     "Geocode (Free Text Search)",
		Description: "This endpoint gives you the ability to search for a feature based on free text search.",
	}, handlers.GeocodeHandler(config))
}

/* func greetHandler(ctx context.Context, input *struct {
	Name string `path:"name" maxLength:"30" example:"world" doc:"Name to greet"`
}) (*GreetingOutput, error) {
	resp := &GreetingOutput{}
	resp.Body.Message = fmt.Sprintf("Hello, %s!", input.Name)
	return resp, nil
} */
