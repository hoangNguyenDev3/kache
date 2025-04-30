package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/hoangNguyenDev3/redis-clone/store"
)

// HTTPServer represents an HTTP server providing a RESTful API
type HTTPServer struct {
	store     *store.Store
	authToken string
	logger    *log.Logger
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(store *store.Store, authToken string) *HTTPServer {
	return &HTTPServer{
		store:     store,
		authToken: authToken,
		logger:    log.New(log.Writer(), "[HTTP] ", log.LstdFlags),
	}
}

// Start starts the HTTP server
func (s *HTTPServer) Start(addr string) error {
	// Set up routes
	mux := http.NewServeMux()

	// String operations
	mux.HandleFunc("/api/string/", s.handleStringOperations)

	// Hash operations
	mux.HandleFunc("/api/hash/", s.handleHashOperations)

	// Key operations
	mux.HandleFunc("/api/keys", s.handleKeysOperation)

	// Info about the server
	mux.HandleFunc("/api/info", s.handleInfoOperation)

	// Wrap with middleware
	handler := s.authMiddleware(mux)
	handler = s.loggingMiddleware(handler)

	s.logger.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, handler)
}

// Middleware to check authentication
func (s *HTTPServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth check if no token is configured
		if s.authToken == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Check for auth token
		token := r.Header.Get("Authorization")
		if token == "" {
			token = r.URL.Query().Get("token")
		}

		if token != s.authToken {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Middleware for logging
func (s *HTTPServer) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// Handle string operations (GET, SET, DELETE)
func (s *HTTPServer) handleStringOperations(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL
	key := strings.TrimPrefix(r.URL.Path, "/api/string/")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		// Get the value
		value, err := s.store.Get(key)
		if err != nil {
			if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
				http.Error(w, "Key not found", http.StatusNotFound)
			} else if err == store.ErrWrongType {
				http.Error(w, "Wrong type", http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		// Prepare response
		response := map[string]interface{}{
			"key":   key,
			"value": value,
		}

		// Send response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

	case "POST", "PUT":
		// Parse request body
		var data struct {
			Value  string `json:"value"`
			Expiry int    `json:"expiry,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Set expiry if provided
		var expiryTime *time.Time
		if data.Expiry > 0 {
			t := time.Now().Add(time.Duration(data.Expiry) * time.Second)
			expiryTime = &t
		}

		// Set the value
		if err := s.store.Set(key, data.Value, expiryTime); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Send response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "OK"})

	case "DELETE":
		// Delete the key
		count, err := s.store.Del(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Send response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]int{"deleted": count})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Handle hash operations (HGET, HSET, HDEL, HGETALL, etc.)
func (s *HTTPServer) handleHashOperations(w http.ResponseWriter, r *http.Request) {
	// Extract key and possibly field from URL
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/hash/"), "/")
	if len(parts) < 1 || parts[0] == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	key := parts[0]
	var field string
	if len(parts) > 1 {
		field = parts[1]
	}

	switch r.Method {
	case "GET":
		if field != "" {
			// HGET
			value, err := s.store.HGet(key, field)
			if err != nil {
				if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
					http.Error(w, "Key or field not found", http.StatusNotFound)
				} else if err == store.ErrWrongType {
					http.Error(w, "Wrong type", http.StatusBadRequest)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"key":   key,
				"field": field,
				"value": value,
			})
		} else {
			// HGETALL
			fields, err := s.store.HGetAll(key)
			if err != nil {
				if err == store.ErrKeyNotFound || err == store.ErrKeyExpired {
					// Return empty hash instead of 404
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]interface{}{
						"key":    key,
						"fields": map[string]string{},
					})
				} else if err == store.ErrWrongType {
					http.Error(w, "Wrong type", http.StatusBadRequest)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"key":    key,
				"fields": fields,
			})
		}

	case "POST", "PUT":
		if field != "" {
			// HSET
			var data struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}

			_, err := s.store.HSet(key, field, data.Value)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
		} else {
			// Bulk HSET
			var data map[string]string
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}

			// Set each field
			for f, v := range data {
				if _, err := s.store.HSet(key, f, v); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]int{"set": len(data)})
		}

	case "DELETE":
		if field != "" {
			// HDEL single field
			count, err := s.store.HDel(key, field)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]int{"deleted": count})
		} else {
			// Delete entire hash
			count, err := s.store.Del(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Send response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]int{"deleted": count})
		}

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Handle KEYS operation
func (s *HTTPServer) handleKeysOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get pattern from query string
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}

	// Get keys
	keys := s.store.Keys(pattern)

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"pattern": pattern,
		"keys":    keys,
		"count":   len(keys),
	})
}

// Handle INFO operation
func (s *HTTPServer) handleInfoOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get store stats
	keyCount := s.store.Size()
	isReplica := s.store.IsReplica()

	// Get server info
	info := map[string]interface{}{
		"redis_clone_version": "1.0.0",
		"uptime":              "N/A", // Would require tracking server start time
		"connected_clients":   0,     // Would require tracking connections
		"used_memory":         "N/A", // Would require memory tracking
		"key_count":           keyCount,
		"is_replica":          isReplica,
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}
