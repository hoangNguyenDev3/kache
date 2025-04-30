package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/hoangNguyenDev3/redis-clone/server"
	"github.com/hoangNguyenDev3/redis-clone/store"
	"github.com/stretchr/testify/assert"
)

func setupTestServer() *server.HTTPServer {
	gin.SetMode(gin.TestMode)
	s := store.New(&store.StoreConfig{})
	return server.NewHTTPServer(s, "test-token")
}

func TestSetValue(t *testing.T) {
	srv := setupTestServer()

	// Test valid request
	body := map[string]interface{}{
		"value": "test-value",
	}
	jsonBody, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v1/kv/test-key", bytes.NewBuffer(jsonBody))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)
	assert.Equal(t, "ok", response["status"])

	// Test invalid auth
	req = httptest.NewRequest("POST", "/v1/kv/test-key", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestGetValue(t *testing.T) {
	srv := setupTestServer()

	// Set up test data
	srv.Store().Set("test-key", "test-value", nil)

	// Test valid request
	req := httptest.NewRequest("GET", "/v1/kv/test-key", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)
	assert.Equal(t, "test-value", response["value"])

	// Test non-existent key
	req = httptest.NewRequest("GET", "/v1/kv/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestDeleteValue(t *testing.T) {
	srv := setupTestServer()

	// Set up test data
	srv.Store().Set("test-key", "test-value", nil)

	// Test valid request
	req := httptest.NewRequest("DELETE", "/v1/kv/test-key", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)
	assert.Equal(t, float64(1), response["deleted"])
}

func TestIncrValue(t *testing.T) {
	srv := setupTestServer()

	// Set up test data
	srv.Store().Set("test-key", int64(5), nil)

	// Test valid request
	req := httptest.NewRequest("POST", "/v1/kv/test-key/incr", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)
	assert.Equal(t, float64(6), response["value"])
}

func TestHashOperations(t *testing.T) {
	srv := setupTestServer()

	// Test HSET
	t.Run("HSET", func(t *testing.T) {
		payload := map[string]string{
			"field": "field1",
			"value": "value1",
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/v1/hash/test-hash", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.True(t, response["created"].(bool))
	})

	// Test HGET
	t.Run("HGET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/hash/test-hash/field1", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "value1", response["value"])
	})
}

func TestMetrics(t *testing.T) {
	srv := setupTestServer()

	// Make some requests to generate metrics
	body := map[string]interface{}{
		"value": "test-value",
	}
	jsonBody, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v1/kv/test-key", bytes.NewBuffer(jsonBody))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	// Get metrics
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w = httptest.NewRecorder()
	srv.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	metricsOutput := w.Body.String()
	assert.Contains(t, metricsOutput, "redis_clone_commands_total")
	assert.Contains(t, metricsOutput, "redis_clone_command_duration_seconds")
	assert.Contains(t, metricsOutput, "redis_clone_connected_clients")
	assert.Contains(t, metricsOutput, "redis_clone_memory_usage_bytes")
}
