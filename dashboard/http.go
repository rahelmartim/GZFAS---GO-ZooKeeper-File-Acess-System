package dashboard

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/rahelmartim/gzfas/coord"
)

const maxBody = 256 * 1024

// Handler serves the dashboard page and its API:
//
//	GET  /api/hello          the store mode
//	POST /api/do/<action>    one action, with a JSON object of strings
//	GET  /api/events         Server-Sent Events: every ZooKeeper call
func Handler(engine *Engine, mode string, files fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(files))
	mux.HandleFunc("GET /api/hello", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]string{"mode": mode}})
	})
	mux.HandleFunc("POST /api/do/{action}", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		args := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "the body must be a JSON object of strings"})
			return
		}
		result, err := engine.Do(r.PathValue("action"), args)
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming is not supported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		// Send the headers now, so that the browser opens the stream at once.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": connected\n\n"))
		flusher.Flush()
		calls := make(chan coord.Call, 256)
		stop := engine.Subscribe(func(c coord.Call) {
			select {
			case calls <- c:
			default: // A slow browser misses calls instead of blocking ZooKeeper work.
			}
		})
		defer stop()
		ping := time.NewTicker(20 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case c := <-calls:
				data, _ := json.Marshal(c)
				_, _ = w.Write([]byte("data: " + string(data) + "\n\n"))
				flusher.Flush()
			case <-ping.C:
				_, _ = w.Write([]byte(": ping\n\n"))
				flusher.Flush()
			}
		}
	})
	return secure(mux)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// secure adds security headers to every response.
func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", strings.Join([]string{
			"default-src 'self'",
			"script-src 'self' 'wasm-unsafe-eval'",
			"style-src 'self' https://fonts.googleapis.com",
			"font-src https://fonts.gstatic.com",
			"connect-src 'self'",
			"img-src 'self' data:",
			"frame-ancestors 'none'",
		}, "; "))
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
