// Package dashboard runs the actions of the GZFAS dashboard on a System.
// The browser demo (WebAssembly) and the HTTP server use the same Engine.
package dashboard

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/rahelmartim/gzfas/coord"
	"github.com/rahelmartim/gzfas/gzfas"
)

// Engine owns a System and records its ZooKeeper calls.
type Engine struct {
	mu       sync.Mutex
	recorder *coord.Recorder
	system   *gzfas.System
	inspect  coord.Session
	nextAddr int
}

// Options configure a new Engine.
type Options struct {
	Store   coord.Store // the raw store; the Engine records it
	Blobs   gzfas.BlobStore
	Config  gzfas.Config
	Servers []string // servers to start, for example s1, s2, s3
}

// New starts a System on the store, with the servers of opts.
func New(opts Options) (*Engine, error) {
	recorder := coord.NewRecorder(opts.Store, 400)
	system, err := gzfas.New(recorder, opts.Blobs, opts.Config)
	if err != nil {
		return nil, err
	}
	inspect, err := system.Inspector()
	if err != nil {
		system.Close()
		return nil, err
	}
	e := &Engine{recorder: recorder, system: system, inspect: inspect}
	for _, id := range opts.Servers {
		if _, err := e.startServer(id); err != nil {
			e.Close()
			return nil, err
		}
	}
	return e, nil
}

// Close ends every session.
func (e *Engine) Close() {
	e.system.Close()
	_ = e.inspect.Close()
}

// Subscribe calls fn for every ZooKeeper call.
func (e *Engine) Subscribe(fn func(coord.Call)) func() { return e.recorder.Subscribe(fn) }

// Calls returns the recorded calls after seq.
func (e *Engine) Calls(after int64) []coord.Call {
	out := []coord.Call{}
	for _, c := range e.recorder.Calls() {
		if c.Seq > after {
			out = append(out, c)
		}
	}
	return out
}

func (e *Engine) startServer(id string) (*gzfas.Server, error) {
	e.mu.Lock()
	e.nextAddr++
	address := fmt.Sprintf("10.0.0.%d:7000", e.nextAddr)
	e.mu.Unlock()
	return e.system.StartServer(id, address)
}

// Do runs one action. Every argument is a string, as it comes from a form.
func (e *Engine) Do(action string, args map[string]string) (any, error) {
	arg := func(key string) string { return args[key] }
	client := func() (*gzfas.Client, error) { return e.system.Client(arg("client")) }

	switch action {
	case "snapshot":
		return e.system.Snapshot(e.inspect)
	case "calls":
		after, _ := strconv.ParseInt(arg("after"), 10, 64)
		return e.Calls(after), nil
	case "connect":
		_, err := e.system.Connect(arg("client"))
		return nil, err
	case "disconnect":
		c, err := client()
		if err != nil {
			return nil, err
		}
		return nil, c.Disconnect()
	case "lock":
		c, err := client()
		if err != nil {
			return nil, err
		}
		l, err := c.RequestLock(arg("file"), gzfas.Mode(arg("mode")))
		if err != nil {
			return nil, err
		}
		return map[string]string{"node": l.Name()}, nil
	case "release":
		c, err := client()
		if err != nil {
			return nil, err
		}
		for _, l := range c.Locks() {
			if l.File == arg("file") {
				return nil, l.Release()
			}
		}
		return nil, errors.New("no lock on this file")
	case "read":
		c, err := client()
		if err != nil {
			return nil, err
		}
		content, meta, err := c.Read(arg("file"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"content": string(content), "meta": meta}, nil
	case "write":
		c, err := client()
		if err != nil {
			return nil, err
		}
		return nil, c.Write(arg("file"), []byte(arg("content")), arg("note"))
	case "rollback":
		c, err := client()
		if err != nil {
			return nil, err
		}
		return nil, c.Rollback(arg("file"), arg("version"))
	case "create":
		c, err := client()
		if err != nil {
			return nil, err
		}
		return nil, c.CreateFile(arg("file"), []byte(arg("content")))
	case "delete":
		c, err := client()
		if err != nil {
			return nil, err
		}
		return nil, c.DeleteFile(arg("file"))
	case "startServer":
		_, err := e.startServer(arg("server"))
		return nil, err
	case "stopServer":
		srv, err := e.system.Server(arg("server"))
		if err != nil {
			return nil, err
		}
		srv.Stop()
		return nil, nil
	case "setConf":
		value, err := strconv.Atoi(arg("value"))
		if err != nil {
			return nil, gzfas.ErrBadConf
		}
		return nil, e.system.SetConf(arg("key"), value)
	default:
		return nil, fmt.Errorf("unknown action %q", action)
	}
}
