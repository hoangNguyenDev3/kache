package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/hoangNguyenDev3/kache/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Version is injected at build time and exposed via the /health endpoint.
var Version string

// HTTPServer provides a REST API over the Kache store, backed by the
// Gin framework. It exposes key-value, hash, and list operations as well
// as Prometheus-compatible metrics and runtime statistics.
type HTTPServer struct {
	store      *store.Store
	engine     *gin.Engine
	authToken  string
	metrics    *Metrics
	registry   *prometheus.Registry
	httpServer *http.Server
	startTime  time.Time
	certFile   string
	keyFile    string
}

// Metrics holds Prometheus metrics exported by the HTTP server.
type Metrics struct {
	commandsTotal    *prometheus.CounterVec
	commandLatency   *prometheus.HistogramVec
	connectedClients prometheus.Gauge
	memoryUsage      prometheus.Gauge
	cacheHitRatio    prometheus.Gauge
	errorRate        *prometheus.CounterVec
	keyspaceSize     prometheus.Gauge
}

// NewHTTPServer creates and returns a new HTTPServer backed by the given store.
// The authToken is required via the Authorization header for all mutating routes.
func NewHTTPServer(store *store.Store, authToken string) *HTTPServer {
	gin.SetMode(gin.ReleaseMode)
	registry := prometheus.NewRegistry()
	metrics := newMetrics()

	s := &HTTPServer{
		store:     store,
		engine:    gin.New(),
		authToken: authToken,
		metrics:   metrics,
		registry:  registry,
		startTime: time.Now(),
	}

	// Register metrics with the custom registry
	registry.MustRegister(
		metrics.commandsTotal,
		metrics.commandLatency,
		metrics.connectedClients,
		metrics.memoryUsage,
		metrics.cacheHitRatio,
		metrics.errorRate,
		metrics.keyspaceSize,
	)

	// CORS middleware for cross-origin requests from the demo frontend
	s.engine.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}))

	s.setupRoutes()
	return s
}

func newMetrics() *Metrics {
	return &Metrics{
		commandsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "redis_clone_commands_total",
				Help: "Total number of commands processed",
			},
			[]string{"command", "status"},
		),
		commandLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "redis_clone_command_duration_seconds",
				Help:    "Command execution latency in seconds",
				Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15),
			},
			[]string{"command"},
		),
		connectedClients: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_clone_connected_clients",
				Help: "Number of connected clients",
			},
		),
		memoryUsage: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_clone_memory_usage_bytes",
				Help: "Current memory usage in bytes",
			},
		),
		cacheHitRatio: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_clone_cache_hit_ratio",
				Help: "Cache hit ratio",
			},
		),
		errorRate: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "redis_clone_errors_total",
				Help: "Total number of errors",
			},
			[]string{"type"},
		),
		keyspaceSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_clone_keyspace_size",
				Help: "Total number of keys in the database",
			},
		),
	}
}

// SetTLS configures TLS certificate and key files for the HTTP server.
func (s *HTTPServer) SetTLS(certFile, keyFile string) {
	s.certFile = certFile
	s.keyFile = keyFile
}

// Start binds the HTTP server to addr and blocks while serving requests.
func (s *HTTPServer) Start(addr string) error {
	if s.certFile != "" && s.keyFile != "" {
		slog.Info("http server listening with TLS", "addr", addr, "component", "http")
	} else {
		slog.Info("http server listening", "addr", addr, "component", "http")
	}
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.engine,
	}
	if s.certFile != "" && s.keyFile != "" {
		return s.httpServer.ListenAndServeTLS(s.certFile, s.keyFile)
	}
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server with the given context.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *HTTPServer) setupRoutes() {
	// Add authentication middleware
	auth := s.engine.Group("/", s.authMiddleware())

	// Key-value operations
	auth.POST("/v1/kv/:key", s.setValue)
	auth.GET("/v1/kv/:key", s.getValue)
	auth.DELETE("/v1/kv/:key", s.deleteValue)
	auth.POST("/v1/kv/:key/incr", s.incrValue)

	// Hash operations
	auth.POST("/v1/hash/:key", s.setHashField)
	auth.GET("/v1/hash/:key/:field", s.getHashField)

	// List operations
	auth.POST("/v1/list/:key/lpush", s.lpushValue)
	auth.POST("/v1/list/:key/rpush", s.rpushValue)
	auth.POST("/v1/list/:key/lpop", s.lpopValue)
	auth.POST("/v1/list/:key/rpop", s.rpopValue)
	auth.GET("/v1/list/:key/len", s.llenValue)
	auth.GET("/v1/list/:key/range", s.lrangeValue)

	// Monitoring endpoints
	s.engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})))
	s.engine.GET("/health", s.getHealth)
	auth.GET("/stats", s.getStats)

	// pprof profiling endpoints
	s.engine.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	s.engine.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	s.engine.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	s.engine.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	s.engine.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	s.engine.GET("/debug/pprof/allocs", gin.WrapH(pprof.Handler("allocs")))
	s.engine.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	s.engine.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	s.engine.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	s.engine.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	s.engine.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
}

func (s *HTTPServer) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.authToken == "" {
			c.Next()
			return
		}
		token := c.GetHeader("Authorization")
		if token != "Bearer "+s.authToken {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid authorization token",
			})
			return
		}
		c.Next()
	}
}

func (s *HTTPServer) setValue(c *gin.Context) {
	start := time.Now()
	key := c.Param("key")
	var value interface{}
	if err := c.BindJSON(&value); err != nil {
		s.metrics.errorRate.WithLabelValues("bind_error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.store.Set(key, value, nil); err != nil {
		s.metrics.errorRate.WithLabelValues("set_error").Inc()
		s.metrics.commandsTotal.WithLabelValues("set", "error").Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("set", "success").Inc()
	s.metrics.commandLatency.WithLabelValues("set").Observe(time.Since(start).Seconds())
	s.updateKeyspaceSize()

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// updateKeyspaceSize updates the keyspace size metric
func (s *HTTPServer) updateKeyspaceSize() {
	keys := s.store.Keys("*")
	s.metrics.keyspaceSize.Set(float64(len(keys)))
}

func (s *HTTPServer) getValue(c *gin.Context) {
	start := time.Now()
	key := c.Param("key")

	value, err := s.store.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
			s.metrics.cacheHitRatio.Set(0)
			s.metrics.commandsTotal.WithLabelValues("get", "miss").Inc()
			c.Status(http.StatusNotFound)
			return
		}
		s.metrics.errorRate.WithLabelValues("get_error").Inc()
		s.metrics.commandsTotal.WithLabelValues("get", "error").Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	s.metrics.cacheHitRatio.Set(1)
	s.metrics.commandsTotal.WithLabelValues("get", "hit").Inc()
	s.metrics.commandLatency.WithLabelValues("get").Observe(time.Since(start).Seconds())

	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (s *HTTPServer) deleteValue(c *gin.Context) {
	key := c.Param("key")

	start := time.Now()
	count, err := s.store.Del(key)
	s.metrics.commandLatency.WithLabelValues("del").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("del", "error").Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("del", "success").Inc()
	s.updateKeyspaceSize()
	c.JSON(http.StatusOK, gin.H{"deleted": count})
}

func (s *HTTPServer) incrValue(c *gin.Context) {
	key := c.Param("key")

	start := time.Now()
	val, err := s.store.Incr(key)
	s.metrics.commandLatency.WithLabelValues("incr").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("incr", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("incr", "success").Inc()
	c.JSON(http.StatusOK, gin.H{"value": val})
}

func (s *HTTPServer) setHashField(c *gin.Context) {
	key := c.Param("key")
	var req struct {
		Field string `json:"field" binding:"required"`
		Value string `json:"value" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		s.metrics.errorRate.WithLabelValues("hset_error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start := time.Now()
	created, err := s.store.HSet(key, req.Field, req.Value)
	s.metrics.commandLatency.WithLabelValues("hset").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("hset", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("hset", "success").Inc()
	c.JSON(http.StatusOK, gin.H{"created": created})
}

func (s *HTTPServer) getHashField(c *gin.Context) {
	key := c.Param("key")
	field := c.Param("field")

	start := time.Now()
	value, err := s.store.HGet(key, field)
	s.metrics.commandLatency.WithLabelValues("hget").Observe(time.Since(start).Seconds())

	if err != nil {
		if err == store.ErrKeyNotFound {
			s.metrics.commandsTotal.WithLabelValues("hget", "miss").Inc()
			c.Status(http.StatusNotFound)
			return
		}
		s.metrics.commandsTotal.WithLabelValues("hget", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("hget", "hit").Inc()
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (s *HTTPServer) lpushValue(c *gin.Context) {
	key := c.Param("key")
	var req struct {
		Values []string `json:"values" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		s.metrics.errorRate.WithLabelValues("lpush_error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start := time.Now()
	length, err := s.store.LPush(key, req.Values...)
	s.metrics.commandLatency.WithLabelValues("lpush").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("lpush", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("lpush", "success").Inc()
	s.updateKeyspaceSize()
	c.JSON(http.StatusOK, gin.H{"length": length})
}

func (s *HTTPServer) rpushValue(c *gin.Context) {
	key := c.Param("key")
	var req struct {
		Values []string `json:"values" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		s.metrics.errorRate.WithLabelValues("rpush_error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start := time.Now()
	length, err := s.store.RPush(key, req.Values...)
	s.metrics.commandLatency.WithLabelValues("rpush").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("rpush", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("rpush", "success").Inc()
	s.updateKeyspaceSize()
	c.JSON(http.StatusOK, gin.H{"length": length})
}

func (s *HTTPServer) lpopValue(c *gin.Context) {
	key := c.Param("key")

	start := time.Now()
	val, err := s.store.LPop(key)
	s.metrics.commandLatency.WithLabelValues("lpop").Observe(time.Since(start).Seconds())

	if err != nil {
		if err == store.ErrKeyNotFound {
			s.metrics.commandsTotal.WithLabelValues("lpop", "miss").Inc()
			c.Status(http.StatusNotFound)
			return
		}
		s.metrics.commandsTotal.WithLabelValues("lpop", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("lpop", "success").Inc()
	s.updateKeyspaceSize()
	c.JSON(http.StatusOK, gin.H{"value": val})
}

func (s *HTTPServer) rpopValue(c *gin.Context) {
	key := c.Param("key")

	start := time.Now()
	val, err := s.store.RPop(key)
	s.metrics.commandLatency.WithLabelValues("rpop").Observe(time.Since(start).Seconds())

	if err != nil {
		if err == store.ErrKeyNotFound {
			s.metrics.commandsTotal.WithLabelValues("rpop", "miss").Inc()
			c.Status(http.StatusNotFound)
			return
		}
		s.metrics.commandsTotal.WithLabelValues("rpop", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("rpop", "success").Inc()
	s.updateKeyspaceSize()
	c.JSON(http.StatusOK, gin.H{"value": val})
}

func (s *HTTPServer) llenValue(c *gin.Context) {
	key := c.Param("key")

	start := time.Now()
	length, err := s.store.LLen(key)
	s.metrics.commandLatency.WithLabelValues("llen").Observe(time.Since(start).Seconds())

	if err != nil {
		s.metrics.commandsTotal.WithLabelValues("llen", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("llen", "success").Inc()
	c.JSON(http.StatusOK, gin.H{"length": length})
}

func (s *HTTPServer) lrangeValue(c *gin.Context) {
	key := c.Param("key")

	startStr := c.Query("start")
	stopStr := c.Query("stop")

	start, err := strconv.Atoi(startStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid start parameter"})
		return
	}

	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid stop parameter"})
		return
	}

	startTime := time.Now()
	values, err := s.store.LRange(key, start, stop)
	s.metrics.commandLatency.WithLabelValues("lrange").Observe(time.Since(startTime).Seconds())

	if err != nil {
		if err == store.ErrKeyNotFound {
			s.metrics.commandsTotal.WithLabelValues("lrange", "miss").Inc()
			c.Status(http.StatusNotFound)
			return
		}
		s.metrics.commandsTotal.WithLabelValues("lrange", "error").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.metrics.commandsTotal.WithLabelValues("lrange", "success").Inc()
	c.JSON(http.StatusOK, gin.H{"values": values})
}

func (s *HTTPServer) getHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":         "ok",
		"version":        Version,
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"go_version":     runtime.Version(),
		"goroutines":     runtime.NumGoroutine(),
	})
}

func (s *HTTPServer) getStats(c *gin.Context) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.metrics.memoryUsage.Set(float64(mem.Alloc))

	// Update keyspace size
	s.updateKeyspaceSize()

	// Count connections (simplified since we don't track clients)
	connectionCount := 1 // Just a placeholder value
	s.metrics.connectedClients.Set(float64(connectionCount))

	// Simplified statistics without replication info
	stats := map[string]interface{}{
		"uptime":            time.Since(s.startTime).Seconds(),
		"connected_clients": connectionCount,
		"keyspace_size":     len(s.store.Keys("*")),
		"memory_usage":      mem.Alloc,
	}

	c.JSON(http.StatusOK, stats)
}

// Engine returns the underlying Gin engine for testing or middleware injection.
func (s *HTTPServer) Engine() *gin.Engine {
	return s.engine
}

// Store returns the Kache store instance used by the HTTP server.
func (s *HTTPServer) Store() *store.Store {
	return s.store
}
