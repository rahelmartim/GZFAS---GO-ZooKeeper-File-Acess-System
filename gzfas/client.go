package gzfas

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/rahelmartim/gzfas/coord"
)

// ClientInfo is the data of /GZFAS/clients/<name>.
type ClientInfo struct {
	Status  string    `json:"status"`
	Since   time.Time `json:"since"`
	Actions int       `json:"actions"`
}

// Client is one user of GZFAS. It has its own ZooKeeper session, so its
// znodes and its locks disappear when it disconnects or crashes.
type Client struct {
	system  *System
	Name    string
	session coord.Session
	mu      sync.Mutex
	locks   map[string]*Lock // by file
	actions int
	closed  bool
	since   time.Time
}

// Connect registers a client with an ephemeral znode, and checks MAX_CONN.
func (s *System) Connect(name string) (*Client, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if _, ok := s.clients[name]; ok {
		s.mu.Unlock()
		return nil, ErrNameTaken
	}
	s.mu.Unlock()

	session, err := s.store.Connect("client " + name)
	if err != nil {
		return nil, err
	}
	s.remember(session, "client "+name)
	c := &Client{system: s, Name: name, session: session, locks: map[string]*Lock{}, since: s.now()}
	info, _ := json.Marshal(ClientInfo{Status: "connected", Since: c.since})
	if _, err := session.Create(coord.Join(ClientsPath, name), info, coord.Ephemeral); err != nil {
		_ = session.Close()
		if errors.Is(err, coord.ErrNodeExists) {
			return nil, ErrNameTaken
		}
		return nil, err
	}
	// Create first, then count: two clients that connect at the same time
	// cannot both pass the limit.
	maxConn, err := s.confValue("MAX_CONN")
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	connected, _, err := session.Children(ClientsPath)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	if len(connected) > maxConn {
		_ = session.Close()
		return nil, ErrTooManyConn
	}
	s.mu.Lock()
	s.clients[name] = c
	s.mu.Unlock()
	return c, nil
}

// Client returns a connected client.
func (s *System) Client(name string) (*Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[name]
	if !ok {
		return nil, ErrNoClient
	}
	return c, nil
}

// Clients returns the connected clients of this process, by name.
func (s *System) Clients() []*Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Client, 0, len(s.clients))
	for _, c := range s.clients {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SessionID returns the ZooKeeper session of the client.
func (c *Client) SessionID() string { return c.session.ID() }

// Disconnect ends the session. ZooKeeper deletes the client znode and every
// lock znode of the client, and the next clients in the queues get the locks.
func (c *Client) Disconnect() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	err := c.session.Close()
	c.system.mu.Lock()
	delete(c.system.clients, c.Name)
	c.system.mu.Unlock()
	return err
}

// Locks returns the lock requests of the client, by file.
func (c *Client) Locks() []*Lock {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Lock, 0, len(c.locks))
	for _, l := range c.locks {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}

// status writes the state of the client to its znode.
func (c *Client) status() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	text := "connected"
	files := make([]string, 0, len(c.locks))
	for file := range c.locks {
		files = append(files, file)
	}
	sort.Strings(files)
	for _, file := range files {
		l := c.locks[file]
		switch l.State() {
		case LockHeld:
			if l.Mode == Read {
				text = "reading " + file
			} else {
				text = "writing " + file
			}
		case LockWaiting:
			if text == "connected" {
				text = "waiting for " + file
			}
		}
	}
	c.actions++
	info, _ := json.Marshal(ClientInfo{Status: text, Since: c.since, Actions: c.actions})
	c.mu.Unlock()
	_, _ = c.session.Set(coord.Join(ClientsPath, c.Name), info, coord.AnyVersion)
}

func (c *Client) lockFor(file string) (*Lock, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locks[file]
	return l, ok
}

func (c *Client) forget(file string, l *Lock) {
	c.mu.Lock()
	if c.locks[file] == l {
		delete(c.locks, file)
	}
	c.mu.Unlock()
}
