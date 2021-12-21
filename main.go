package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uncle-lv/logger"
)

type key int

const (
	Attempts key = iota
	Retry
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Alive = alive
}

func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	defer b.mux.RUnlock()
	alive = b.Alive
	return
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (s *ServerPool) next() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) MarkBackendStatus(url *url.URL, alive bool) {
	for _, b := range s.backends {
		if b.URL.String() == url.String() {
			b.SetAlive(alive)
			break
		}
	}
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.next()
	l := len(s.backends) + next
	for i := next; i < l; i++ {
		index := i % len(s.backends)
		if s.backends[index].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(index))
			}
			return s.backends[index]
		}
	}
	return nil
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "UP"
		alive := isBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "DOWN"
		}
		logger.Debugf("%s [%s]\n", b.URL, status)
	}
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func roundRobin(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	peer := serverPool.GetNextPeer()
	if peer != nil {
		logger.Debug("distribute the request to ", peer.URL)
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

func isBackendAlive(url *url.URL) bool {
	timeout := 3 * time.Second
	conn, err := net.DialTimeout("tcp", url.Host, timeout)
	if err != nil {
		logger.Error("Backend unreachable, error: ", err)
		return false
	}
	defer conn.Close()
	return true
}

func healthCheck() {
	t := time.NewTicker(1 * time.Minute)
	for range t.C {
		logger.Debug("Start health check...")
		serverPool.HealthCheck()
		logger.Debug("Health check compeleted")
	}
}

var serverPool ServerPool

func main() {
	urls := [...]string{
		"http://localhost:3000",
		"http://localhost:3001",
		"http://localhost:3002",
	}

	for _, ur := range urls {
		serverUrl, err := url.Parse(ur)
		if err != nil {
			logger.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(rw http.ResponseWriter, r *http.Request, e error) {
			logger.Errorf("[%s] %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(r)
			if retries < 3 {
				time.After(10 * time.Millisecond)
				ctx := context.WithValue(r.Context(), Retry, retries+1)
				proxy.ServeHTTP(rw, r.WithContext(ctx))
				return
			}

			serverPool.MarkBackendStatus(serverUrl, false)
			attempts := GetAttemptsFromContext(r)
			ctx := context.WithValue(r.Context(), Attempts, attempts+1)
			roundRobin(rw, r.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})

		logger.Info("Configured server: ", serverUrl)
	}

	server := http.Server{
		Addr:    ":8000",
		Handler: http.HandlerFunc(roundRobin),
	}

	go healthCheck()

	logger.Info("Load Balacer started at", server.Addr)
	if err := server.ListenAndServe(); err != nil {
		logger.Fatal(err)
	}
}
