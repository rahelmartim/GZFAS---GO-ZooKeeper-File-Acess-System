package dashboard

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/rahelmartim/gzfas/coord"
	"github.com/rahelmartim/gzfas/gzfas"
)

func newEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(Options{Store: coord.NewMemoryStore(), Config: gzfas.Config{MaxConn: 5, MaxRead: 2}, Servers: []string{"s1", "s2"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func do(t *testing.T, e *Engine, action string, args map[string]string) any {
	t.Helper()
	result, err := e.Do(action, args)
	if err != nil {
		t.Fatalf("%s %v: %v", action, args, err)
	}
	return result
}

func snapshot(t *testing.T, e *Engine) gzfas.Snapshot {
	t.Helper()
	return do(t, e, "snapshot", nil).(gzfas.Snapshot)
}

func TestEngineFullStory(t *testing.T) {
	e := newEngine(t)
	do(t, e, "connect", map[string]string{"client": "ana"})
	do(t, e, "connect", map[string]string{"client": "bia"})
	do(t, e, "create", map[string]string{"client": "ana", "file": "book.txt", "content": "v1"})
	do(t, e, "lock", map[string]string{"client": "ana", "file": "book.txt", "mode": "write"})
	waitFor(t, e, func(s gzfas.Snapshot) bool { return len(s.Files) == 1 && s.Files[0].Status == "write" })
	do(t, e, "write", map[string]string{"client": "ana", "file": "book.txt", "content": "v2", "note": "edit"})
	do(t, e, "lock", map[string]string{"client": "bia", "file": "book.txt", "mode": "read"})
	do(t, e, "release", map[string]string{"client": "ana", "file": "book.txt"})
	waitFor(t, e, func(s gzfas.Snapshot) bool { return s.Files[0].Status == "read" })
	read := do(t, e, "read", map[string]string{"client": "bia", "file": "book.txt"}).(map[string]any)
	if read["content"] != "v2" {
		t.Fatalf("read = %v", read)
	}
	do(t, e, "setConf", map[string]string{"key": "MAX_READ", "value": "4"})
	do(t, e, "stopServer", map[string]string{"server": "s1"})
	waitFor(t, e, func(s gzfas.Snapshot) bool { return s.Leader != nil && s.Leader.ID == "s2" })
	do(t, e, "startServer", map[string]string{"server": "s3"})
	do(t, e, "disconnect", map[string]string{"client": "bia"})
	waitFor(t, e, func(s gzfas.Snapshot) bool { return s.Files[0].Status == "free" && len(s.Clients) == 1 })
	if s := snapshot(t, e); s.Conf["MAX_READ"] != 4 || len(s.Files[0].Versions) != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func waitFor(t *testing.T, e *Engine, ok func(gzfas.Snapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok(snapshot(t, e)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out; snapshot = %+v", snapshot(t, e))
}

func TestEngineErrors(t *testing.T) {
	e := newEngine(t)
	for _, c := range []struct {
		action string
		args   map[string]string
	}{
		{"nope", nil},
		{"lock", map[string]string{"client": "ghost", "file": "x", "mode": "read"}},
		{"setConf", map[string]string{"key": "MAX_READ", "value": "many"}},
		{"stopServer", map[string]string{"server": "s9"}},
		{"connect", map[string]string{"client": "../bad"}},
	} {
		if _, err := e.Do(c.action, c.args); err == nil {
			t.Errorf("%s %v: no error", c.action, c.args)
		}
	}
}

func TestEngineRecordsCalls(t *testing.T) {
	e := newEngine(t)
	before := len(e.Calls(0))
	do(t, e, "connect", map[string]string{"client": "ana"})
	calls := e.Calls(0)
	found := false
	for _, c := range calls[before:] {
		if strings.HasPrefix(c.Command, "create -e /GZFAS/clients/ana '") && c.Actor == "client ana" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no create of the client znode in %+v", calls[before:])
	}
	// The dashboard reads do not fill the log.
	n := len(e.Calls(0))
	snapshot(t, e)
	if len(e.Calls(0)) != n {
		t.Fatal("a snapshot added calls")
	}
	if last := calls[len(calls)-1]; len(e.Calls(last.Seq)) != 0 {
		t.Fatal("Calls(after) returned old calls")
	}
}

func server(t *testing.T) (*Engine, *httptest.Server) {
	e := newEngine(t)
	files := fstest.MapFS{"index.html": {Data: []byte("<h1>GZFAS</h1>")}}
	srv := httptest.NewServer(Handler(e, "memory", files))
	t.Cleanup(srv.Close)
	// Cleanups run in reverse order: first end the open event streams.
	t.Cleanup(srv.CloseClientConnections)
	return e, srv
}

func post(t *testing.T, srv *httptest.Server, action, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(srv.URL+"/api/do/"+action, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHTTPAPI(t *testing.T) {
	_, srv := server(t)
	if out := post(t, srv, "connect", `{"client":"ana"}`); out["ok"] != true {
		t.Fatalf("connect: %v", out)
	}
	if out := post(t, srv, "connect", `{"client":"ana"}`); out["ok"] != false || !strings.Contains(out["error"].(string), "already") {
		t.Fatalf("second connect: %v", out)
	}
	out := post(t, srv, "snapshot", `{}`)
	clients := out["result"].(map[string]any)["clients"].([]any)
	if len(clients) != 1 {
		t.Fatalf("clients = %v", clients)
	}

	// Actions need POST. A GET only reaches the static files.
	resp, _ := http.Get(srv.URL + "/api/do/snapshot")
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("GET ran an action: %d", resp.StatusCode)
	}
	resp, _ = http.Post(srv.URL+"/api/do/connect", "application/json", strings.NewReader(`{"client": 5}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad body: %d", resp.StatusCode)
	}
	big := `{"client":"` + strings.Repeat("a", maxBody) + `"}`
	resp, _ = http.Post(srv.URL+"/api/do/connect", "application/json", strings.NewReader(big))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("large body: %d", resp.StatusCode)
	}
}

func TestHTTPPageAndHeaders(t *testing.T) {
	_, srv := server(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Fatalf("page: %d %v", resp.StatusCode, resp.Header)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("no nosniff header")
	}
	hello, _ := http.Get(srv.URL + "/api/hello")
	var out map[string]any
	_ = json.NewDecoder(hello.Body).Decode(&out)
	if out["result"].(map[string]any)["mode"] != "memory" {
		t.Fatalf("hello = %v", out)
	}
}

func TestHTTPEventsStreamTheCalls(t *testing.T) {
	_, srv := server(t)
	resp, err := http.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	go func() {
		resp, err := http.Post(srv.URL+"/api/do/connect", "application/json", strings.NewReader(`{"client":"zoe"}`))
		if err == nil {
			resp.Body.Close()
		}
	}()
	reader := bufio.NewReader(resp.Body)
	deadline := time.After(5 * time.Second)
	lines := make(chan string)
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			lines <- line
		}
	}()
	for {
		select {
		case line := <-lines:
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, "/GZFAS/clients/zoe") {
				return
			}
		case <-deadline:
			t.Fatal("no event for the new client")
		}
	}
}
