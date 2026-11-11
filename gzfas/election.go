package gzfas

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"

	"github.com/rahelmartim/gzfas/coord"
)

// Server is one GZFAS server. The servers elect a leader with the election
// recipe of ZooKeeper:
//
//  1. Create an ephemeral sequential znode: /GZFAS/servers/n-.
//  2. The server with the lowest number is the leader. It writes the
//     ephemeral znode /GZFAS/leader.
//  3. Every other server watches only the znode right before its own.
//     When that znode goes, the server checks again, and maybe leads.
//
// When the leader stops or crashes, its session ends, its znodes go, and
// the next server becomes the leader. Clients only work while a leader exists.
type Server struct {
	system  *System
	ID      string
	Address string
	session coord.Session
	Node    string

	mu       sync.Mutex
	leader   bool
	watching string
	stopped  bool
	done     chan struct{}
}

// ServerInfo is the data of an election znode.
type ServerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// StartServer joins the election.
func (s *System) StartServer(id, address string) (*Server, error) {
	if err := ValidateName(id); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if _, ok := s.servers[id]; ok {
		s.mu.Unlock()
		return nil, ErrServerExists
	}
	s.mu.Unlock()
	session, err := s.store.Connect("server " + id)
	if err != nil {
		return nil, err
	}
	s.remember(session, "server "+id)
	data, _ := json.Marshal(ServerInfo{ID: id, Address: address})
	node, err := session.Create(coord.Join(ServersPath, "n-"), data, coord.EphemeralSequential)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	srv := &Server{system: s, ID: id, Address: address, session: session, Node: node, done: make(chan struct{})}
	s.mu.Lock()
	s.servers[id] = srv
	s.mu.Unlock()
	ready := make(chan struct{})
	go srv.run(ready)
	<-ready
	return srv, nil
}

// Server returns a server of this process.
func (s *System) Server(id string) (*Server, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	srv, ok := s.servers[id]
	if !ok {
		return nil, ErrNoServer
	}
	return srv, nil
}

// Servers returns the servers of this process, by ID.
func (s *System) Servers() []*Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Server, 0, len(s.servers))
	for _, srv := range s.servers {
		out = append(out, srv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// IsLeader reports whether this server leads now.
func (srv *Server) IsLeader() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.leader
}

// Watching returns the election znode that this server watches.
func (srv *Server) Watching() string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.watching
}

// SessionID returns the ZooKeeper session of the server.
func (srv *Server) SessionID() string { return srv.session.ID() }

// Stop ends the session of the server, as a crash would.
func (srv *Server) Stop() {
	srv.mu.Lock()
	if srv.stopped {
		srv.mu.Unlock()
		return
	}
	srv.stopped = true
	srv.mu.Unlock()
	_ = srv.session.Close()
	<-srv.done
	srv.system.mu.Lock()
	delete(srv.system.servers, srv.ID)
	srv.system.mu.Unlock()
}

func (srv *Server) run(ready chan struct{}) {
	defer close(srv.done)
	once := sync.Once{}
	signal := func() { once.Do(func() { close(ready) }) }
	defer signal()
	me := coord.Base(srv.Node)
	for {
		names, _, err := srv.session.Children(ServersPath)
		if err != nil {
			return
		}
		sortBySequence(names)
		index := sort.SearchStrings(names, me)
		if index >= len(names) || names[index] != me {
			return // Our znode is gone: the session ended.
		}
		if index == 0 {
			srv.lead()
			signal()
			// A leader has nothing to watch. It leads until its session ends.
			_, _, events, err := srv.session.ExistsW(srv.Node)
			if err == nil {
				<-events
			}
			return
		}
		previous := names[index-1]
		srv.mu.Lock()
		srv.watching = previous
		srv.mu.Unlock()
		exists, _, events, err := srv.session.ExistsW(coord.Join(ServersPath, previous))
		signal()
		if err != nil {
			return
		}
		if !exists {
			continue
		}
		if event := <-events; event.Type == coord.SessionClosed {
			return
		}
	}
}

func (srv *Server) lead() {
	data, _ := json.Marshal(LeaderInfo{ID: srv.ID, Address: srv.Address, Since: srv.system.now()})
	for {
		_, err := srv.session.Create(LeaderPath, data, coord.Ephemeral)
		if err == nil || !errors.Is(err, coord.ErrNodeExists) {
			break
		}
		// The znode of the old leader is still there: wait until it goes.
		exists, _, events, err := srv.session.ExistsW(LeaderPath)
		if err != nil {
			return
		}
		if exists {
			<-events
		}
	}
	srv.mu.Lock()
	srv.leader, srv.watching = true, ""
	srv.mu.Unlock()
}
