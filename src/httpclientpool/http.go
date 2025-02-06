package httpclientpool

import (
	"net/http"
	"sync"
	"time"
)

type HTTPClientPool struct {
	client    *http.Client
	transport *http.Transport
	once      sync.Once
}

var poolInstance *HTTPClientPool

// GetPoolInstance returns a singleton instance of HTTPClientPool
func GetPoolInstance() *HTTPClientPool {
	if poolInstance == nil {
		poolInstance = &HTTPClientPool{}
	}
	return poolInstance
}

// GetClient initializes (if needed) and returns the shared HTTP client.
func (pool *HTTPClientPool) GetClient() *http.Client {
	pool.once.Do(func() {
		// Configure the transport
		pool.transport = &http.Transport{
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		}

		// Create the shared HTTP client
		pool.client = &http.Client{
			Timeout:   10 * time.Second,
			Transport: pool.transport,
		}
	})
	return pool.client
}

// Close cleans up resources by closing idle connections
func (pool *HTTPClientPool) Close() {
	if pool.transport != nil {
		pool.transport.CloseIdleConnections()
	}
}
