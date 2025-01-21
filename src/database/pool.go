package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

var (
	dbPoolMap       = make(map[string]*pgxpool.Pool) // Map to store database connection pools
	dbPoolMutex     sync.Mutex                       // Mutex to ensure thread safety for dbPoolMap
	poolLastUsed    = make(map[string]time.Time)     // Map to track last usage time of each pool
	cleanupInterval = 1 * time.Minute                // Interval to check for idle pools
)

// init is called before the main function.
// It starts a goroutine to periodically clean up idle database connection pools.
func init() {
	go periodicCleanup()
}

// periodicCleanup is a goroutine that periodically cleans up idle database connection pools.
// It closes idle pools that have not been used for a certain duration specified by cleanupInterval.
func periodicCleanup() {
	for {
		time.Sleep(cleanupInterval)

		dbPoolMutex.Lock()
		for name, pool := range dbPoolMap {
			lastUsed, ok := poolLastUsed[name]
			if !ok || time.Since(lastUsed) > cleanupInterval {
				pool.Close()
				delete(dbPoolMap, name)
				delete(poolLastUsed, name)
				log.Debugf("Closed idle database pool: %s", name)
			}
		}
		dbPoolMutex.Unlock()
	}
}

// CloseDBPools closes all the database connection pools.
// It acquires a lock on the dbPoolMutex to ensure thread safety.
// It iterates over all the pools in the dbPoolMap and calls the Close method on each pool.
// After closing the pools, it resets the dbPoolMap and poolLastUsed to empty maps.
func CloseDBPools() {
	dbPoolMutex.Lock()
	defer dbPoolMutex.Unlock()
	for _, pool := range dbPoolMap {
		pool.Close()
	}
	dbPoolMap = make(map[string]*pgxpool.Pool)
	poolLastUsed = make(map[string]time.Time)
}

// GetDBPool returns a database connection pool for the specified name and connection string.
// If a pool with the given name already exists, it returns the existing pool.
// Otherwise, it creates a new pool and adds it to the pool map.
// The last used time for the pool is updated each time it is retrieved or created.
func GetDBPool(name string, connectionString string) (*pgxpool.Pool, error) {
	dbPoolMutex.Lock()
	defer dbPoolMutex.Unlock()

	if pool, ok := dbPoolMap[name]; ok {
		poolLastUsed[name] = time.Now() // Update last used time
		return pool, nil
	}

	poolConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		log.Fatalf("Unable to parse connection string: %v\n", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)

	if err != nil {
		return nil, fmt.Errorf("error connecting to database '%s': %v", name, err)
	}

	err = pool.Ping(context.Background())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("error connecting to database '%s': %v", name, err)
	}

	log.Debugf("Opened new database pool: %s", name)
	dbPoolMap[name] = pool
	poolLastUsed[name] = time.Now() // Update last used time
	return pool, nil
}
