package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type APIMock struct {
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Response json.RawMessage `json:"response"`
	Status   int             `json:"status"`
	Enabled  bool            `json:"enabled"`
}

var (
	apiMocks   = make(map[string]APIMock)
	mocksMutex sync.RWMutex
)

// Intercept and serve mock if exists
func interceptAndMockAPI(w http.ResponseWriter, r *http.Request) bool {
	key := r.Method + " " + r.URL.Path
	mocksMutex.RLock()
	mock, exists := apiMocks[key]
	mocksMutex.RUnlock()
	if exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(mock.Status)
		w.Write(mock.Response)
		log.Printf("Mocked API: %s %s", r.Method, r.URL.Path)
		return true
	}
	return false
}
