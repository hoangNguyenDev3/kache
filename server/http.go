package server

import (
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hoangNguyenDev3/redis-clone/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer represents an HTTP server that provides a REST API
type HTTPServer struct {
	store     *store.Store
	engine    *gin.Engine
	authToken string
	metrics   *Metrics
	registry  *prometheus.Registry
}

// Metrics holds Prometheus metrics
type Metrics struct {
	commandsTotal    *prometheus.CounterVec
	commandLatency   *prometheus.HistogramVec
	connectedClients prometheus.Gauge
	memoryUsage      prometheus.Gauge
	cacheHitRatio    prometheus.Gauge
	errorRate        *prometheus.CounterVec
	keyspaceSize     prometheus.Gauge
}

// NewHTTPServer creates a new HTTP server instance
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

// Start starts the HTTP server
func (s *HTTPServer) Start(addr string) error {
	return s.engine.Run(addr)
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

	// Monitoring endpoints
	s.engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})))
	auth.GET("/stats", s.getStats)
}

func (s *HTTPServer) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
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
		"uptime":            time.Since(startTime).Seconds(),
		"connected_clients": connectionCount,
		"keyspace_size":     len(s.store.Keys("*")),
		"memory_usage":      mem.Alloc,
	}

	c.JSON(http.StatusOK, stats)
}

// Engine returns the underlying Gin engine
func (s *HTTPServer) Engine() *gin.Engine {
	return s.engine
}

// Store returns the underlying store
func (s *HTTPServer) Store() *store.Store {
	return s.store
}

// Store the server start time
var startTime = time.Now()
