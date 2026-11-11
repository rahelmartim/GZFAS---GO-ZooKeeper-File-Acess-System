//go:build js && wasm

// Command gzfas-wasm runs GZFAS in the browser on an in-memory ZooKeeper.
// It exposes a global "gzfas" object:
//
//	gzfas.do(action, argsJSON) -> Promise<resultJSON>
//	gzfas.reset()             -> Promise, starts again with a new tree
//	gzfas.onCall = function (callJSON) { ... }  // every ZooKeeper call
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/rahelmartim/gzfas/coord"
	"github.com/rahelmartim/gzfas/dashboard"
	"github.com/rahelmartim/gzfas/gzfas"
)

var (
	engine *dashboard.Engine
	api    js.Value
)

func start() error {
	e, err := dashboard.New(dashboard.Options{
		Store:   coord.NewMemoryStore(),
		Config:  gzfas.Config{MaxConn: 8, MaxRead: 3},
		Servers: []string{"s1", "s2", "s3"},
	})
	if err != nil {
		return err
	}
	e.Subscribe(func(call coord.Call) {
		handler := api.Get("onCall")
		if handler.Type() == js.TypeFunction {
			data, _ := json.Marshal(call)
			handler.Invoke(string(data))
		}
	})
	engine = e
	return nil
}

// promise runs fn in a goroutine, because the actions wait on channels.
func promise(fn func() (string, error)) js.Value {
	var executor js.Func
	executor = js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve := args[0]
		go func() {
			defer executor.Release()
			out, err := fn()
			if err != nil {
				data, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				resolve.Invoke(string(data))
				return
			}
			resolve.Invoke(out)
		}()
		return nil
	})
	return js.Global().Get("Promise").New(executor)
}

func main() {
	api = js.Global().Get("Object").New()
	api.Set("do", js.FuncOf(func(_ js.Value, args []js.Value) any {
		action := args[0].String()
		raw := args[1].String()
		return promise(func() (string, error) {
			params := map[string]string{}
			if err := json.Unmarshal([]byte(raw), &params); err != nil {
				return "", err
			}
			result, err := engine.Do(action, params)
			if err != nil {
				return "", err
			}
			data, err := json.Marshal(map[string]any{"ok": true, "result": result})
			return string(data), err
		})
	}))
	api.Set("reset", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		return promise(func() (string, error) {
			engine.Close()
			if err := start(); err != nil {
				return "", err
			}
			return `{"ok":true}`, nil
		})
	}))
	if err := start(); err != nil {
		panic(err)
	}
	js.Global().Set("gzfas", api)
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("gzfas-ready"))
	select {}
}
