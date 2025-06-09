package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed admin.html
var adminHTML []byte

//go:embed api-tools.html
var apiToolsHTML []byte

//go:embed api-config.html
var apiConfigHTML []byte

type RouteConfig struct {
	Path string `json:"path"`
	Port int    `json:"port"`
	Name string `json:"name"`
}

type CORSConfig struct {
	Enabled          bool     `json:"enabled"`
	AllowOrigins     []string `json:"allow_origins"`
	AllowMethods     []string `json:"allow_methods"`
	AllowHeaders     []string `json:"allow_headers"`
	AllowCredentials bool     `json:"allow_credentials"`
	MaxAge           int      `json:"max_age"`
}

type ProxyConfig struct {
	FrontendPort     int           `json:"frontend_port"`
	BackendPort      int           `json:"backend_port"`
	DevPort          int           `json:"dev_port"`
	BackendPaths     []string      `json:"backend_paths"`
	CustomRoutes     []RouteConfig `json:"custom_routes"`
	CORS             CORSConfig    `json:"cors"`
	PassAllHeaders   bool          `json:"pass_all_headers"`
	PassCookies      bool          `json:"pass_cookies"`
	HotReloadEnabled bool          `json:"hot_reload_enabled"`
	HotReloadPaths   []string      `json:"hot_reload_paths"`
	mu               sync.RWMutex
}

type APIRequestLog struct {
	Timestamp int64  `json:"timestamp"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Body      string `json:"body,omitempty"`
}

var (
	config           = &ProxyConfig{}
	frontendProxy    *httputil.ReverseProxy
	backendProxy     *httputil.ReverseProxy
	devProxy         *httputil.ReverseProxy
	customProxies    map[string]*httputil.ReverseProxy
	backendPattern   *regexp.Regexp
	customPatterns   map[string]*regexp.Regexp
	watcher          *fsnotify.Watcher
	hotReloadClients []chan string
	clientsMutex     sync.RWMutex
	db               *sql.DB

	apiRequests  []APIRequestLog
	apiReqsMutex sync.RWMutex
)

func main() {
	if err := initDB(); err != nil {
		log.Fatalf("Failed to initialize DB: %v", err)
	}
	if err := loadConfigFromDB(); err != nil {
		log.Printf("Failed to load config from DB: %v", err)
	}

	updateProxies()
	setupHotReload()

	http.HandleFunc("/", mainHandler)

	// Graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{
		Addr:    ":8787",
		Handler: nil,
	}

	go func() {
		fmt.Printf("🚀 Advanced Proxy Server Starting on port 8787\n")
		fmt.Printf("📊 Admin UI: http://localhost:8787/admin\n")
		fmt.Printf("🛠️ API Tools: http://localhost:8787/api-tools\n")
		fmt.Printf("⚙️ Config: http://localhost:8787/api-config\n")
		fmt.Printf("🔧 API: http://localhost:8787/proxy-api/\n")
		fmt.Printf("🎯 Routing:\n")
		fmt.Printf("   Frontend: :%d | Backend: :%d | Dev: :%d\n",
			config.FrontendPort, config.BackendPort, config.DevPort)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-c
	fmt.Println("\n🛑 Shutting down proxy server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	if watcher != nil {
		watcher.Close()
	}

	fmt.Println("✅ Proxy server stopped")
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	// Track all API requests for admin
	if strings.HasPrefix(r.URL.Path, "/proxy-api/") && !strings.HasPrefix(r.URL.Path, "/proxy-api/intercept-log") {
		body := ""
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			body = string(b)
			r.Body = io.NopCloser(strings.NewReader(body)) // restore for downstream
		}
		apiReqsMutex.Lock()
		apiRequests = append(apiRequests, APIRequestLog{
			Timestamp: time.Now().Unix(),
			Method:    r.Method,
			Path:      r.URL.Path,
			Body:      body,
		})
		apiReqsMutex.Unlock()
	}

	config.mu.RLock()
	ready := config.FrontendPort != 0 && config.BackendPort != 0 && config.DevPort != 0
	config.mu.RUnlock()

	if !ready &&
		!strings.HasPrefix(r.URL.Path, "/proxy-api/") &&
		!strings.HasPrefix(r.URL.Path, "/admin") &&
		!strings.HasPrefix(r.URL.Path, "/api-tools") &&
		!strings.HasPrefix(r.URL.Path, "/api-config") {
		http.Error(w, "Proxy not configured. Please POST config to /proxy-api/config.", http.StatusServiceUnavailable)
		return
	}

	// Serve admin UI
	if r.URL.Path == "/admin" || r.URL.Path == "/admin/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(adminHTML)
		return
	}

	// Serve API tools UI
	if r.URL.Path == "/api-tools" || r.URL.Path == "/api-tools/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(apiToolsHTML)
		return
	}

	// Serve API config UI
	if r.URL.Path == "/api-config" || r.URL.Path == "/api-config/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(apiConfigHTML)
		return
	}

	// --- API Management ---
	if strings.HasPrefix(r.URL.Path, "/proxy-api/") {
		switch {
		case r.URL.Path == "/proxy-api/list":
			apiListHandler(w, r)
		case r.URL.Path == "/proxy-api/config":
			if r.Method == "GET" {
				getConfigHandler(w, r)
			} else if r.Method == "POST" {
				updateConfigHandler(w, r)
			}
		case r.URL.Path == "/proxy-api/test":
			testRouteHandler(w, r)
		case r.URL.Path == "/proxy-api/hot-reload":
			hotReloadHandler(w, r)
		case r.URL.Path == "/proxy-api/intercept-log":
			apiInterceptLogHandler(w, r)
		// --- Mock API Management ---
		case r.URL.Path == "/proxy-api/_mock":
			if r.Method == "POST" {
				apiMockHandler(w, r)
			} else if r.Method == "GET" {
				apiMockListHandler(w, r)
			}
		default:
			// Serve mock if exists for this method/path
			if strings.HasPrefix(r.URL.Path, "/proxy-api/_mock/") {
				if serveMockIfExists(w, r) {
					return
				}
				http.NotFound(w, r)
				return
			}
			http.NotFound(w, r)
		}
		return
	}

	// Handle CORS preflight
	if r.Method == "OPTIONS" {
		handleCORSPreflight(w, r)
		return
	}

	config.mu.RLock()
	frontendPort := config.FrontendPort
	backendPort := config.BackendPort
	devPort := config.DevPort
	customRoutes := config.CustomRoutes
	config.mu.RUnlock()

	// Check custom routes
	for _, route := range customRoutes {
		if pattern, exists := customPatterns[route.Path]; exists && pattern.MatchString(r.URL.Path) {
			log.Printf("Custom: %s %s -> :%d (%s)", r.Method, r.URL.Path, route.Port, route.Name)
			customProxies[route.Path].ServeHTTP(w, r)
			return
		}
	}

	// Check backend routes
	if backendPattern != nil && backendPattern.MatchString(r.URL.Path) {
		log.Printf("Backend: %s %s -> :%d", r.Method, r.URL.Path, backendPort)
		backendProxy.ServeHTTP(w, r)
	} else if r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/static") || strings.HasPrefix(r.URL.Path, "/assets") {
		log.Printf("Frontend: %s %s -> :%d", r.Method, r.URL.Path, frontendPort)
		frontendProxy.ServeHTTP(w, r)
	} else {
		log.Printf("Dev: %s %s -> :%d", r.Method, r.URL.Path, devPort)
		if r.Header.Get("Upgrade") == "websocket" {
			r.Header.Set("Connection", "upgrade")
		}
		devProxy.ServeHTTP(w, r)
	}
}

// --- API Management Handlers ---

func apiListHandler(w http.ResponseWriter, r *http.Request) {
	endpoints := []string{
		"GET /proxy-api/config",
		"POST /proxy-api/config",
		"GET /proxy-api/test",
		"GET /proxy-api/hot-reload",
		"GET /proxy-api/list",
		"POST /proxy-api/_mock",
		"GET /proxy-api/_mock",
	}
	mocksMutex.RLock()
	for _, mock := range apiMocks {
		if mock.Enabled {
			endpoints = append(endpoints, fmt.Sprintf("%s %s (mocked)", mock.Method, mock.Path))
		}
	}
	mocksMutex.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"endpoints": endpoints,
	})
}

func apiMockHandler(w http.ResponseWriter, r *http.Request) {
	var mock APIMock
	if err := json.NewDecoder(r.Body).Decode(&mock); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	key := strings.ToUpper(mock.Method) + " " + mock.Path
	mocksMutex.Lock()
	apiMocks[key] = mock
	mocksMutex.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "mock registered"})
}

func apiMockListHandler(w http.ResponseWriter, r *http.Request) {
	mocksMutex.RLock()
	defer mocksMutex.RUnlock()
	list := []APIMock{}
	for _, m := range apiMocks {
		list = append(list, m)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"mocks": list})
}

func serveMockIfExists(w http.ResponseWriter, r *http.Request) bool {
	key := strings.ToUpper(r.Method) + " " + r.URL.Path
	mocksMutex.RLock()
	mock, exists := apiMocks[key]
	mocksMutex.RUnlock()
	if exists && mock.Enabled {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(mock.Status)
		w.Write(mock.Response)
		return true
	}
	return false
}

func apiInterceptLogHandler(w http.ResponseWriter, r *http.Request) {
	apiReqsMutex.RLock()
	defer apiReqsMutex.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"logs": apiRequests})
}

// --- Existing config, proxy, hot reload, and utility functions below (unchanged) ---

func init() {
	customProxies = make(map[string]*httputil.ReverseProxy)
	customPatterns = make(map[string]*regexp.Regexp)
	hotReloadClients = make([]chan string, 0)
}

func initDB() error {
	var err error
	db, err = sql.Open("sqlite3", "./proxyconfig.db")
	if err != nil {
		return err
	}
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS config (
        id INTEGER PRIMARY KEY,
        frontend_port INTEGER,
        backend_port INTEGER,
        dev_port INTEGER,
        config_json TEXT
    )`)
	return err
}

func loadConfigFromDB() error {
	row := db.QueryRow("SELECT frontend_port, backend_port, dev_port, config_json FROM config WHERE id = 1")
	var frontendPort, backendPort, devPort int
	var configJSON string
	err := row.Scan(&frontendPort, &backendPort, &devPort, &configJSON)
	if err == sql.ErrNoRows {
		return nil // No config yet
	}
	if err != nil {
		return err
	}
	var loadedConfig ProxyConfig
	if err := json.Unmarshal([]byte(configJSON), &loadedConfig); err != nil {
		return err
	}
	config.mu.Lock()
	*config = loadedConfig
	config.FrontendPort = frontendPort
	config.BackendPort = backendPort
	config.DevPort = devPort
	config.mu.Unlock()
	return nil
}

func updateProxies() {
	config.mu.Lock()
	defer config.mu.Unlock()

	frontendURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", config.FrontendPort))
	frontendProxy = createReverseProxy(frontendURL)

	backendURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", config.BackendPort))
	backendProxy = createReverseProxy(backendURL)

	devURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", config.DevPort))
	devProxy = createReverseProxy(devURL)

	if len(config.BackendPaths) > 0 {
		pathPattern := strings.Join(config.BackendPaths, "|")
		backendPattern = regexp.MustCompile(fmt.Sprintf(`^/(%s)/`, pathPattern))
	}

	customProxies = make(map[string]*httputil.ReverseProxy)
	customPatterns = make(map[string]*regexp.Regexp)
	for _, route := range config.CustomRoutes {
		customURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", route.Port))
		customProxies[route.Path] = createReverseProxy(customURL)
		customPatterns[route.Path] = regexp.MustCompile(fmt.Sprintf(`^%s`, route.Path))
	}
}

func createReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Custom transport with retry logic
	proxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var conn net.Conn
			var err error
			for i := 0; i < 10; i++ { // Try 10 times
				conn, err = (&net.Dialer{}).DialContext(ctx, network, addr)
				if err == nil {
					return conn, nil
				}
				time.Sleep(1 * time.Second) // Wait before retry
			}
			return nil, err
		},
	}

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		config.mu.RLock()
		passHeaders := config.PassAllHeaders
		passCookies := config.PassCookies
		config.mu.RUnlock()

		req.Header.Set("X-Real-IP", getClientIP(req))
		req.Header.Set("X-Forwarded-For", getClientIP(req))
		req.Header.Set("X-Forwarded-Proto", "http")
		req.Header.Set("X-Forwarded-Host", req.Host)

		if passHeaders {
			for name, values := range req.Header {
				if !strings.HasPrefix(strings.ToLower(name), "x-forwarded") {
					for _, value := range values {
						req.Header.Add(name, value)
					}
				}
			}
		}

		if passCookies {
			if cookies := req.Header.Get("Cookie"); cookies != "" {
				req.Header.Set("Cookie", cookies)
			}
		}
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		config.mu.RLock()
		corsConfig := config.CORS
		passCookies := config.PassCookies
		config.mu.RUnlock()

		if corsConfig.Enabled {
			if len(corsConfig.AllowOrigins) > 0 {
				if corsConfig.AllowOrigins[0] == "*" {
					resp.Header.Set("Access-Control-Allow-Origin", "*")
				} else {
					resp.Header.Set("Access-Control-Allow-Origin", strings.Join(corsConfig.AllowOrigins, ", "))
				}
			}

			if len(corsConfig.AllowMethods) > 0 {
				resp.Header.Set("Access-Control-Allow-Methods", strings.Join(corsConfig.AllowMethods, ", "))
			}

			if len(corsConfig.AllowHeaders) > 0 {
				if corsConfig.AllowHeaders[0] == "*" {
					resp.Header.Set("Access-Control-Allow-Headers", "*")
				} else {
					resp.Header.Set("Access-Control-Allow-Headers", strings.Join(corsConfig.AllowHeaders, ", "))
				}
			}

			if corsConfig.AllowCredentials {
				resp.Header.Set("Access-Control-Allow-Credentials", "true")
			}

			if corsConfig.MaxAge > 0 {
				resp.Header.Set("Access-Control-Max-Age", strconv.Itoa(corsConfig.MaxAge))
			}
		}

		if passCookies {
			for _, cookie := range resp.Header["Set-Cookie"] {
				resp.Header.Add("Set-Cookie", cookie)
			}
		}

		return nil
	}

	return proxy
}

func handleCORSPreflight(w http.ResponseWriter, r *http.Request) {
	config.mu.RLock()
	corsConfig := config.CORS
	config.mu.RUnlock()

	if !corsConfig.Enabled {
		return
	}

	if len(corsConfig.AllowOrigins) > 0 {
		if corsConfig.AllowOrigins[0] == "*" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			origin := r.Header.Get("Origin")
			for _, allowedOrigin := range corsConfig.AllowOrigins {
				if allowedOrigin == origin {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}
		}
	}

	if len(corsConfig.AllowMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(corsConfig.AllowMethods, ", "))
	}

	if len(corsConfig.AllowHeaders) > 0 {
		if corsConfig.AllowHeaders[0] == "*" {
			w.Header().Set("Access-Control-Allow-Headers", "*")
		} else {
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(corsConfig.AllowHeaders, ", "))
		}
	}

	if corsConfig.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if corsConfig.MaxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(corsConfig.MaxAge))
	}

	w.WriteHeader(http.StatusOK)
}

// API Handlers
func getConfigHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config.mu.RLock()
	defer config.mu.RUnlock()
	json.NewEncoder(w).Encode(config)
}

func updateConfigHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newConfig ProxyConfig
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	config.mu.Lock()
	config.FrontendPort = newConfig.FrontendPort
	config.BackendPort = newConfig.BackendPort
	config.DevPort = newConfig.DevPort
	config.BackendPaths = newConfig.BackendPaths
	config.CustomRoutes = newConfig.CustomRoutes
	config.CORS = newConfig.CORS
	config.PassAllHeaders = newConfig.PassAllHeaders
	config.PassCookies = newConfig.PassCookies
	config.HotReloadEnabled = newConfig.HotReloadEnabled
	config.HotReloadPaths = newConfig.HotReloadPaths
	config.mu.Unlock()

	updateProxies()

	if config.HotReloadEnabled {
		setupHotReload()
	}

	log.Printf("Configuration updated via API")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func testRouteHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "Missing path parameter", http.StatusBadRequest)
		return
	}

	// Make a test request to the path
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost%s", path))

	result := map[string]interface{}{
		"path":      path,
		"timestamp": time.Now().Unix(),
	}

	if err != nil {
		result["status"] = "error"
		result["error"] = err.Error()
	} else {
		result["status"] = "success"
		result["status_code"] = resp.StatusCode
		resp.Body.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func hotReloadHandler(w http.ResponseWriter, r *http.Request) {
	config.mu.RLock()
	enabled := config.HotReloadEnabled
	config.mu.RUnlock()

	if !enabled {
		http.Error(w, "Hot reload disabled", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan string, 10)

	clientsMutex.Lock()
	hotReloadClients = append(hotReloadClients, clientChan)
	clientsMutex.Unlock()

	defer func() {
		clientsMutex.Lock()
		for i, ch := range hotReloadClients {
			if ch == clientChan {
				hotReloadClients = append(hotReloadClients[:i], hotReloadClients[i+1:]...)
				break
			}
		}
		clientsMutex.Unlock()
		close(clientChan)
	}()

	fmt.Fprintf(w, "data: {\"type\":\"connected\"}\n\n")
	w.(http.Flusher).Flush()

	for {
		select {
		case message := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", message)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func setupHotReload() {
	config.mu.RLock()
	enabled := config.HotReloadEnabled
	paths := config.HotReloadPaths
	config.mu.RUnlock()

	if !enabled {
		return
	}

	if watcher != nil {
		watcher.Close()
	}

	var err error
	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create file watcher: %v", err)
		return
	}

	go func() {
		defer watcher.Close()
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Printf("File changed: %s", event.Name)
					notifyHotReload(fmt.Sprintf("{\"type\":\"reload\",\"file\":\"%s\"}", event.Name))
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Watcher error: %v", err)
			}
		}
	}()

	for _, path := range paths {
		addPathRecursively(path)
	}
}

func addPathRecursively(root string) {
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			watcher.Add(path)
		}
		return nil
	})
}

func notifyHotReload(message string) {
	clientsMutex.RLock()
	defer clientsMutex.RUnlock()

	for _, clientChan := range hotReloadClients {
		select {
		case clientChan <- message:
		default:
		}
	}
}

func getClientIP(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}
	return strings.Split(r.RemoteAddr, ":")[0]
}
