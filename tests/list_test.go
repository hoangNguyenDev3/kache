package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/hoangNguyenDev3/kache/server"
	"github.com/hoangNguyenDev3/kache/store"
	"github.com/stretchr/testify/assert"
)

func TestStore_LPush_LRange(t *testing.T) {
	s := store.New(nil)

	// LPush single value
	length, err := s.LPush("list1", "a")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	// LPush multiple values
	length, err = s.LPush("list1", "b", "c")
	assert.NoError(t, err)
	assert.Equal(t, 3, length)

	// LRange should return [c, b, a]
	values, err := s.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, values)

	// LRange with sub-range
	values, err = s.LRange("list1", 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "a"}, values)
}

func TestStore_RPush_LRange(t *testing.T) {
	s := store.New(nil)

	// RPush single value
	length, err := s.RPush("list1", "a")
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	// RPush multiple values
	length, err = s.RPush("list1", "b", "c")
	assert.NoError(t, err)
	assert.Equal(t, 3, length)

	// LRange should return [a, b, c]
	values, err := s.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, values)
}

func TestStore_LPop_RPop(t *testing.T) {
	s := store.New(nil)

	// Setup list [a, b, c]
	_, err := s.RPush("list1", "a", "b", "c")
	assert.NoError(t, err)

	// LPop returns "a"
	val, err := s.LPop("list1")
	assert.NoError(t, err)
	assert.Equal(t, "a", val)

	// RPop returns "c"
	val, err = s.RPop("list1")
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// Remaining list should be [b]
	values, err := s.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b"}, values)
}

func TestStore_LLen(t *testing.T) {
	s := store.New(nil)

	// LLen on non-existent key returns 0
	length, err := s.LLen("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, 0, length)

	// Setup list
	_, err = s.RPush("list1", "a", "b", "c")
	assert.NoError(t, err)

	length, err = s.LLen("list1")
	assert.NoError(t, err)
	assert.Equal(t, 3, length)
}

func TestStore_LIndex(t *testing.T) {
	s := store.New(nil)

	// Setup list [a, b, c]
	_, err := s.RPush("list1", "a", "b", "c")
	assert.NoError(t, err)

	// Positive indices
	val, err := s.LIndex("list1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "a", val)

	val, err = s.LIndex("list1", 2)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	// Negative indices
	val, err = s.LIndex("list1", -1)
	assert.NoError(t, err)
	assert.Equal(t, "c", val)

	val, err = s.LIndex("list1", -2)
	assert.NoError(t, err)
	assert.Equal(t, "b", val)

	// Out of range
	_, err = s.LIndex("list1", 10)
	assert.Equal(t, store.ErrKeyNotFound, err)
}

func TestStore_LTrim(t *testing.T) {
	s := store.New(nil)

	// Setup list [a, b, c, d, e]
	_, err := s.RPush("list1", "a", "b", "c", "d", "e")
	assert.NoError(t, err)

	// Trim to [b, c, d]
	err = s.LTrim("list1", 1, 3)
	assert.NoError(t, err)

	values, err := s.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d"}, values)

	// Trim with negative indices to [c, d]
	err = s.LTrim("list1", -2, -1)
	assert.NoError(t, err)

	values, err = s.LRange("list1", 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "d"}, values)
}

func TestStore_ListWrongType(t *testing.T) {
	s := store.New(nil)

	// Set a string key
	err := s.Set("key", "value", nil)
	assert.NoError(t, err)

	// Try LPush on string key
	_, err = s.LPush("key", "a")
	assert.Equal(t, store.ErrWrongType, err)

	// Try RPush on string key
	_, err = s.RPush("key", "a")
	assert.Equal(t, store.ErrWrongType, err)

	// Try LPop on string key
	_, err = s.LPop("key")
	assert.Equal(t, store.ErrWrongType, err)

	// Try RPop on string key
	_, err = s.RPop("key")
	assert.Equal(t, store.ErrWrongType, err)

	// Try LLen on string key
	_, err = s.LLen("key")
	assert.Equal(t, store.ErrWrongType, err)

	// Try LRange on string key
	_, err = s.LRange("key", 0, -1)
	assert.Equal(t, store.ErrWrongType, err)

	// Try LIndex on string key
	_, err = s.LIndex("key", 0)
	assert.Equal(t, store.ErrWrongType, err)

	// Try LTrim on string key
	err = s.LTrim("key", 0, -1)
	assert.Equal(t, store.ErrWrongType, err)
}

func TestStore_ListEmptyDeletion(t *testing.T) {
	s := store.New(nil)

	// Setup list with one element
	_, err := s.RPush("list1", "a")
	assert.NoError(t, err)

	// Pop the only element
	val, err := s.LPop("list1")
	assert.NoError(t, err)
	assert.Equal(t, "a", val)

	// Key should be deleted
	_, err = s.LPop("list1")
	assert.Equal(t, store.ErrKeyNotFound, err)

	// Setup another list
	_, err = s.RPush("list2", "a", "b")
	assert.NoError(t, err)

	// Trim to empty
	err = s.LTrim("list2", 1, 0)
	assert.NoError(t, err)

	// Key should be deleted
	_, err = s.LPop("list2")
	assert.Equal(t, store.ErrKeyNotFound, err)
}

func TestHTTPListOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := store.New(&store.StoreConfig{})
	srv := server.NewHTTPServer(s, "test-token")

	t.Run("LPUSH", func(t *testing.T) {
		payload := map[string][]string{
			"values": []string{"x", "y"},
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/v1/list/test-list/lpush", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, float64(2), response["length"])
	})

	t.Run("RPUSH", func(t *testing.T) {
		payload := map[string][]string{
			"values": []string{"z"},
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/v1/list/test-list/rpush", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, float64(3), response["length"])
	})

	t.Run("LLEN", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/list/test-list/len", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, float64(3), response["length"])
	})

	t.Run("LRANGE", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/list/test-list/range?start=0&stop=-1", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{"y", "x", "z"}, response["values"])
	})

	t.Run("LPOP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/list/test-list/lpop", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "y", response["value"])
	})

	t.Run("RPOP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/list/test-list/rpop", nil)
		req.Header.Set("Authorization", "Bearer test-token")

		w := httptest.NewRecorder()
		srv.Engine().ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "z", response["value"])
	})
}
