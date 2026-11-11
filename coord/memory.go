package coord

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore keeps a znode tree in memory with the ZooKeeper rules:
//
//   - every change gets a new transaction ID (zxid)
//   - Set and Delete can require a version
//   - sequential znodes get a 10-digit counter from their parent
//   - ephemeral znodes cannot have children, and disappear with their session
//   - a watch fires once, for the first matching change
type MemoryStore struct {
	mu       sync.Mutex
	zxid     int64
	nodes    map[string]*memNode
	watches  map[string][]*watch // data watches (Get, Exists)
	cwatches map[string][]*watch // child watches (Children)
	sessions map[string]*memSession
	nextID   int64
	now      func() time.Time
}

type memNode struct {
	data     []byte
	stat     Stat
	children map[string]bool
	sequence int32
}

type watch struct {
	ch      chan Event
	session string
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns a store that contains only the root znode "/".
func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		nodes:    map[string]*memNode{},
		watches:  map[string][]*watch{},
		cwatches: map[string][]*watch{},
		sessions: map[string]*memSession{},
		now:      time.Now,
	}
	s.nodes["/"] = &memNode{children: map[string]bool{}}
	return s
}

// Connect opens a session. The session ID looks like a ZooKeeper one.
func (s *MemoryStore) Connect(name string) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	session := &memSession{store: s, id: fmt.Sprintf("0x%x", 0x100000000+s.nextID), name: name}
	s.sessions[session.id] = session
	return session, nil
}

// fire sends one event to every watch of a path and removes those watches.
func fire(list map[string][]*watch, path string, event Event) {
	for _, w := range list[path] {
		w.ch <- event
	}
	delete(list, path)
}

func newWatch(session string) *watch {
	return &watch{ch: make(chan Event, 1), session: session}
}

type memSession struct {
	store  *MemoryStore
	id     string
	name   string
	closed bool
}

func (m *memSession) ID() string { return m.id }

func (m *memSession) check(path string) error {
	if m.closed {
		return ErrSessionClosed
	}
	return ValidatePath(path)
}

func (m *memSession) Create(path string, data []byte, mode Mode) (string, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return "", err
	}
	if path == "/" {
		return "", ErrNodeExists
	}
	parentPath := Parent(path)
	parent, ok := s.nodes[parentPath]
	if !ok {
		return "", ErrNoNode
	}
	if parent.stat.EphemeralOwner != "" {
		return "", ErrNoChildrenForEphemerals
	}
	if mode&Sequential != 0 {
		path = fmt.Sprintf("%s%010d", path, parent.sequence)
	}
	if _, exists := s.nodes[path]; exists {
		return "", ErrNodeExists
	}
	s.zxid++
	now := s.now()
	node := &memNode{data: append([]byte(nil), data...), children: map[string]bool{}}
	node.stat = Stat{Czxid: s.zxid, Mzxid: s.zxid, Ctime: now, Mtime: now}
	if mode&Ephemeral != 0 {
		node.stat.EphemeralOwner = m.id
	}
	s.nodes[path] = node
	parent.children[Base(path)] = true
	parent.sequence++
	parent.stat.Cversion++
	parent.stat.NumChildren++

	fire(s.watches, path, Event{NodeCreated, path})
	fire(s.cwatches, parentPath, Event{NodeChildrenChanged, parentPath})
	return path, nil
}

func (m *memSession) get(path string, watchIt bool) ([]byte, Stat, <-chan Event, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return nil, Stat{}, nil, err
	}
	node, ok := s.nodes[path]
	if !ok {
		return nil, Stat{}, nil, ErrNoNode
	}
	var ch <-chan Event
	if watchIt {
		w := newWatch(m.id)
		s.watches[path] = append(s.watches[path], w)
		ch = w.ch
	}
	return append([]byte(nil), node.data...), node.stat, ch, nil
}

func (m *memSession) Get(path string) ([]byte, Stat, error) {
	data, stat, _, err := m.get(path, false)
	return data, stat, err
}

func (m *memSession) GetW(path string) ([]byte, Stat, <-chan Event, error) {
	return m.get(path, true)
}

func (m *memSession) exists(path string, watchIt bool) (bool, Stat, <-chan Event, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return false, Stat{}, nil, err
	}
	var ch <-chan Event
	if watchIt {
		// An exists watch also fires when a missing znode is created.
		w := newWatch(m.id)
		s.watches[path] = append(s.watches[path], w)
		ch = w.ch
	}
	node, ok := s.nodes[path]
	if !ok {
		return false, Stat{}, ch, nil
	}
	return true, node.stat, ch, nil
}

func (m *memSession) Exists(path string) (bool, Stat, error) {
	ok, stat, _, err := m.exists(path, false)
	return ok, stat, err
}

func (m *memSession) ExistsW(path string) (bool, Stat, <-chan Event, error) {
	return m.exists(path, true)
}

func (m *memSession) children(path string, watchIt bool) ([]string, Stat, <-chan Event, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return nil, Stat{}, nil, err
	}
	node, ok := s.nodes[path]
	if !ok {
		return nil, Stat{}, nil, ErrNoNode
	}
	names := make([]string, 0, len(node.children))
	for name := range node.children {
		names = append(names, name)
	}
	sort.Strings(names)
	var ch <-chan Event
	if watchIt {
		w := newWatch(m.id)
		s.cwatches[path] = append(s.cwatches[path], w)
		ch = w.ch
	}
	return names, node.stat, ch, nil
}

func (m *memSession) Children(path string) ([]string, Stat, error) {
	names, stat, _, err := m.children(path, false)
	return names, stat, err
}

func (m *memSession) ChildrenW(path string) ([]string, Stat, <-chan Event, error) {
	return m.children(path, true)
}

func (m *memSession) Set(path string, data []byte, version int32) (Stat, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return Stat{}, err
	}
	node, ok := s.nodes[path]
	if !ok {
		return Stat{}, ErrNoNode
	}
	if version != AnyVersion && version != node.stat.Version {
		return Stat{}, ErrBadVersion
	}
	s.zxid++
	node.data = append([]byte(nil), data...)
	node.stat.Version++
	node.stat.Mzxid = s.zxid
	node.stat.Mtime = s.now()
	fire(s.watches, path, Event{NodeDataChanged, path})
	return node.stat, nil
}

func (m *memSession) Delete(path string, version int32) error {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := m.check(path); err != nil {
		return err
	}
	return s.deleteLocked(path, version)
}

func (s *MemoryStore) deleteLocked(path string, version int32) error {
	if path == "/" {
		return ErrBadPath
	}
	node, ok := s.nodes[path]
	if !ok {
		return ErrNoNode
	}
	if version != AnyVersion && version != node.stat.Version {
		return ErrBadVersion
	}
	if len(node.children) > 0 {
		return ErrNotEmpty
	}
	s.zxid++
	delete(s.nodes, path)
	parentPath := Parent(path)
	parent := s.nodes[parentPath]
	delete(parent.children, Base(path))
	parent.stat.Cversion++
	parent.stat.NumChildren--

	fire(s.watches, path, Event{NodeDeleted, path})
	fire(s.cwatches, path, Event{NodeDeleted, path})
	fire(s.cwatches, parentPath, Event{NodeChildrenChanged, parentPath})
	return nil
}

// Close deletes the ephemeral znodes of the session, as ZooKeeper does when
// a session ends, and ends its pending watches.
func (m *memSession) Close() error {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	delete(s.sessions, m.id)

	var owned []string
	for path, node := range s.nodes {
		if node.stat.EphemeralOwner == m.id {
			owned = append(owned, path)
		}
	}
	sort.Strings(owned)
	for _, path := range owned {
		_ = s.deleteLocked(path, AnyVersion)
	}
	for _, list := range []map[string][]*watch{s.watches, s.cwatches} {
		for path, watches := range list {
			kept := watches[:0]
			for _, w := range watches {
				if w.session == m.id {
					w.ch <- Event{SessionClosed, path}
				} else {
					kept = append(kept, w)
				}
			}
			if len(kept) == 0 {
				delete(list, path)
			} else {
				list[path] = kept
			}
		}
	}
	return nil
}

// Sessions returns the open sessions: session ID -> name.
func (s *MemoryStore) Sessions() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for id, session := range s.sessions {
		out[id] = session.name
	}
	return out
}

// WatchCount returns the number of pending watches, for tests and the demo.
func (s *MemoryStore) WatchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, list := range []map[string][]*watch{s.watches, s.cwatches} {
		for _, watches := range list {
			n += len(watches)
		}
	}
	return n
}

// Watches returns the paths that have pending watches, and the sessions
// that set them.
func (s *MemoryStore) Watches() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]string{}
	for _, list := range []map[string][]*watch{s.watches, s.cwatches} {
		for path, watches := range list {
			for _, w := range watches {
				out[path] = append(out[path], w.session)
			}
		}
	}
	for path := range out {
		sort.Strings(out[path])
	}
	return out
}

// Paths returns every znode path in order. It is useful in tests.
func (s *MemoryStore) Paths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := make([]string, 0, len(s.nodes))
	for path := range s.nodes {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool { return strings.Compare(paths[i], paths[j]) < 0 })
	return paths
}
