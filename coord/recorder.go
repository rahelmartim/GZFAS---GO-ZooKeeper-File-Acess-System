package coord

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Call is one ZooKeeper operation, as the zkCli shell would show it.
type Call struct {
	Seq     int64     `json:"seq"`
	Time    time.Time `json:"time"`
	Session string    `json:"session"`
	Actor   string    `json:"actor"`   // name of the session owner, for example "client ana"
	Command string    `json:"command"` // for example "create -e -s /GZFAS/data/a/locks/read-"
	Result  string    `json:"result"`  // for example "read-0000000003" or "node does not exist"
	Error   bool      `json:"error"`
	Kind    string    `json:"kind"` // create, get, set, delete, children, exists, watch, close
}

// Recorder wraps a store and keeps the last calls in memory. Subscribers
// get each call as it happens.
type Recorder struct {
	store       Store
	mu          sync.Mutex
	calls       []Call
	next        int64
	limit       int
	subscribers map[int]func(Call)
	nextSub     int
}

var _ Store = (*Recorder)(nil)

// NewRecorder keeps the last limit calls.
func NewRecorder(store Store, limit int) *Recorder {
	return &Recorder{store: store, limit: limit, subscribers: map[int]func(Call){}}
}

func (r *Recorder) Connect(name string) (Session, error) {
	session, err := r.store.Connect(name)
	if err != nil {
		return nil, err
	}
	rs := &recordedSession{Session: session, recorder: r, actor: name}
	r.add(rs, "connect", fmt.Sprintf("connect  # %s", name), session.ID(), nil)
	return rs, nil
}

// Subscribe calls fn for every new call. It returns a function that stops it.
func (r *Recorder) Subscribe(fn func(Call)) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextSub++
	id := r.nextSub
	r.subscribers[id] = fn
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.subscribers, id)
	}
}

// Calls returns the kept calls, oldest first.
func (r *Recorder) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Call(nil), r.calls...)
}

// Store returns the wrapped store.
func (r *Recorder) Store() Store { return r.store }

func (r *Recorder) add(s *recordedSession, kind, command, result string, err error) {
	r.mu.Lock()
	r.next++
	call := Call{
		Seq: r.next, Time: time.Now(), Session: s.Session.ID(), Actor: s.actor,
		Kind: kind, Command: command, Result: result,
	}
	if err != nil {
		call.Result, call.Error = err.Error(), true
	}
	r.calls = append(r.calls, call)
	if len(r.calls) > r.limit {
		r.calls = r.calls[len(r.calls)-r.limit:]
	}
	subscribers := make([]func(Call), 0, len(r.subscribers))
	for _, fn := range r.subscribers {
		subscribers = append(subscribers, fn)
	}
	r.mu.Unlock()
	for _, fn := range subscribers {
		fn(call)
	}
}

type recordedSession struct {
	Session
	recorder *Recorder
	actor    string
}

func flags(mode Mode) string {
	var parts []string
	if mode&Ephemeral != 0 {
		parts = append(parts, "-e")
	}
	if mode&Sequential != 0 {
		parts = append(parts, "-s")
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ") + " "
}

func short(data []byte) string {
	text := string(data)
	if len(text) > 40 {
		text = text[:40] + "…"
	}
	return text
}

func version(v int32) string {
	if v == AnyVersion {
		return ""
	}
	return fmt.Sprintf(" -v %d", v)
}

func (s *recordedSession) Create(path string, data []byte, mode Mode) (string, error) {
	created, err := s.Session.Create(path, data, mode)
	command := fmt.Sprintf("create %s%s", flags(mode), path)
	if len(data) > 0 {
		command += " '" + short(data) + "'"
	}
	s.recorder.add(s, "create", command, created, err)
	return created, err
}

func (s *recordedSession) Get(path string) ([]byte, Stat, error) {
	data, stat, err := s.Session.Get(path)
	s.recorder.add(s, "get", "get "+path, short(data), err)
	return data, stat, err
}

// fired logs a watch event when ZooKeeper delivers it, and passes it on.
func (s *recordedSession) fired(in <-chan Event) <-chan Event {
	if in == nil {
		return nil
	}
	out := make(chan Event, 1)
	go func() {
		event := <-in
		if event.Type != SessionClosed {
			s.recorder.add(s, "event", fmt.Sprintf("WATCHER::%s %s", event.Type, event.Path), "notified", nil)
		}
		out <- event
	}()
	return out
}

func (s *recordedSession) GetW(path string) ([]byte, Stat, <-chan Event, error) {
	data, stat, ch, err := s.Session.GetW(path)
	s.recorder.add(s, "watch", "get -w "+path, short(data), err)
	return data, stat, s.fired(ch), err
}

func (s *recordedSession) Exists(path string) (bool, Stat, error) {
	ok, stat, err := s.Session.Exists(path)
	s.recorder.add(s, "exists", "stat "+path, existsResult(ok), err)
	return ok, stat, err
}

func (s *recordedSession) ExistsW(path string) (bool, Stat, <-chan Event, error) {
	ok, stat, ch, err := s.Session.ExistsW(path)
	s.recorder.add(s, "watch", "stat -w "+path, existsResult(ok), err)
	return ok, stat, s.fired(ch), err
}

func existsResult(ok bool) string {
	if ok {
		return "exists"
	}
	return "does not exist"
}

func (s *recordedSession) Children(path string) ([]string, Stat, error) {
	names, stat, err := s.Session.Children(path)
	s.recorder.add(s, "children", "ls "+path, "["+strings.Join(names, ", ")+"]", err)
	return names, stat, err
}

func (s *recordedSession) ChildrenW(path string) ([]string, Stat, <-chan Event, error) {
	names, stat, ch, err := s.Session.ChildrenW(path)
	s.recorder.add(s, "watch", "ls -w "+path, "["+strings.Join(names, ", ")+"]", err)
	return names, stat, s.fired(ch), err
}

func (s *recordedSession) Set(path string, data []byte, v int32) (Stat, error) {
	stat, err := s.Session.Set(path, data, v)
	result := ""
	if err == nil {
		result = fmt.Sprintf("version %d", stat.Version)
	}
	s.recorder.add(s, "set", fmt.Sprintf("set %s '%s'%s", path, short(data), version(v)), result, err)
	return stat, err
}

func (s *recordedSession) Delete(path string, v int32) error {
	err := s.Session.Delete(path, v)
	s.recorder.add(s, "delete", "delete "+path+version(v), "", err)
	return err
}

func (s *recordedSession) Close() error {
	err := s.Session.Close()
	s.recorder.add(s, "close", "close  # session ends, ephemeral znodes go", "", err)
	return err
}
