// Package gzfas is a file access system coordinated by ZooKeeper.
//
// Files live under /GZFAS/data. ZooKeeper keeps their metadata, their
// version history, and their locks. The file content lives in a blob store,
// because a znode holds at most about 1 MB.
//
// The znode tree:
//
//	/GZFAS/conf/MAX_CONN            clients that can connect at the same time
//	/GZFAS/conf/MAX_READ            readers of one file at the same time
//	/GZFAS/servers/n-0000000000     ephemeral sequential: leader election
//	/GZFAS/leader                   ephemeral: the current leader
//	/GZFAS/clients/<name>           ephemeral: one per connected client
//	/GZFAS/data/<file>              file metadata (current version)
//	/GZFAS/data/<file>/locks/read-0000000003    ephemeral sequential: lock queue
//	/GZFAS/data/<file>/versions/v-0000000001    sequential: version history
package gzfas

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rahelmartim/gzfas/coord"
)

// Paths of the znode tree.
const (
	Root        = "/GZFAS"
	ConfPath    = Root + "/conf"
	ServersPath = Root + "/servers"
	LeaderPath  = Root + "/leader"
	ClientsPath = Root + "/clients"
	DataPath    = Root + "/data"
)

// Limits on what users send.
const (
	MaxContentBytes = 64 * 1024
	MaxNoteBytes    = 200
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,47}$`)

// Errors that the system returns.
var (
	ErrBadName       = errors.New("names use 1 to 48 letters, digits, dots, hyphens, or underscores")
	ErrTooLarge      = fmt.Errorf("content is larger than %d KB", MaxContentBytes/1024)
	ErrTooManyConn   = errors.New("MAX_CONN reached: too many clients are connected")
	ErrNameTaken     = errors.New("a client with this name is already connected")
	ErrNoClient      = errors.New("client is not connected")
	ErrNoFile        = errors.New("file does not exist")
	ErrFileExists    = errors.New("file already exists")
	ErrNotLocked     = errors.New("this needs the write lock of the file")
	ErrAlreadyLocked = errors.New("the client already has a lock request on this file")
	ErrNoLeader      = errors.New("no GZFAS server is the leader now")
	ErrBadConf       = errors.New("MAX_CONN and MAX_READ must be between 1 and 99")
	ErrNoServer      = errors.New("server does not exist")
	ErrServerExists  = errors.New("server already exists")
)

// ValidateName checks a client, server, or file name.
func ValidateName(name string) error {
	if !nameRe.MatchString(name) || name == "." || name == ".." {
		return ErrBadName
	}
	return nil
}

// Config holds the defaults written to /GZFAS/conf on the first start.
type Config struct {
	MaxConn int
	MaxRead int
}

// System is one GZFAS installation on one ZooKeeper ensemble.
type System struct {
	store   coord.Store
	admin   coord.Session
	blobs   BlobStore
	now     func() time.Time
	mu      sync.Mutex
	clients map[string]*Client
	servers map[string]*Server
	actors  map[string]string // session ID -> actor name
}

// New connects to the store and creates the znode tree if it is missing.
func New(store coord.Store, blobs BlobStore, conf Config) (*System, error) {
	if conf.MaxConn == 0 {
		conf.MaxConn = 8
	}
	if conf.MaxRead == 0 {
		conf.MaxRead = 3
	}
	if blobs == nil {
		blobs = NewMemoryBlobs()
	}
	admin, err := store.Connect("gzfas admin")
	if err != nil {
		return nil, err
	}
	s := &System{
		store: store, admin: admin, blobs: blobs, now: time.Now,
		clients: map[string]*Client{}, servers: map[string]*Server{},
		actors: map[string]string{admin.ID(): "gzfas admin"},
	}
	for _, path := range []string{ConfPath, ServersPath, ClientsPath, DataPath} {
		if err := coord.EnsurePath(admin, path); err != nil {
			return nil, err
		}
	}
	defaults := map[string]int{"MAX_CONN": conf.MaxConn, "MAX_READ": conf.MaxRead}
	for key, value := range defaults {
		_, err := admin.Create(coord.Join(ConfPath, key), []byte(strconv.Itoa(value)), coord.Persistent)
		if err != nil && !errors.Is(err, coord.ErrNodeExists) {
			return nil, err
		}
	}
	return s, nil
}

// Close ends every session of this process.
func (s *System) Close() {
	s.mu.Lock()
	clients := make([]*Client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	servers := make([]*Server, 0, len(s.servers))
	for _, srv := range s.servers {
		servers = append(servers, srv)
	}
	s.mu.Unlock()
	for _, c := range clients {
		_ = c.Disconnect()
	}
	for _, srv := range servers {
		srv.Stop()
	}
	_ = s.admin.Close()
}

func (s *System) remember(session coord.Session, actor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actors[session.ID()] = actor
}

// Actor returns the name of the owner of a session ID.
func (s *System) Actor(sessionID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name, ok := s.actors[sessionID]; ok {
		return name
	}
	return sessionID
}

/* Configuration */

func (s *System) confValue(key string) (int, error) {
	data, _, err := s.admin.Get(coord.Join(ConfPath, key))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

// SetConf changes MAX_CONN or MAX_READ in ZooKeeper.
func (s *System) SetConf(key string, value int) error {
	if (key != "MAX_CONN" && key != "MAX_READ") || value < 1 || value > 99 {
		return ErrBadConf
	}
	_, err := s.admin.Set(coord.Join(ConfPath, key), []byte(strconv.Itoa(value)), coord.AnyVersion)
	return err
}

/* Leader */

// LeaderInfo is the data of /GZFAS/leader.
type LeaderInfo struct {
	ID      string    `json:"id"`
	Address string    `json:"address"`
	Since   time.Time `json:"since"`
}

// Leader reads the current leader from ZooKeeper.
func (s *System) Leader() (LeaderInfo, error) {
	data, _, err := s.admin.Get(LeaderPath)
	if errors.Is(err, coord.ErrNoNode) {
		return LeaderInfo{}, ErrNoLeader
	}
	if err != nil {
		return LeaderInfo{}, err
	}
	var info LeaderInfo
	err = json.Unmarshal(data, &info)
	return info, err
}

/* Helpers */

func filePath(name string) string     { return coord.Join(DataPath, name) }
func locksPath(name string) string    { return coord.Join(DataPath, name, "locks") }
func versionsPath(name string) string { return coord.Join(DataPath, name, "versions") }

// sequenceOf returns the 10-digit counter at the end of a sequential name.
func sequenceOf(name string) int64 {
	if len(name) < 10 {
		return -1
	}
	n, err := strconv.ParseInt(name[len(name)-10:], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// sortBySequence orders sequential znode names by their counter.
func sortBySequence(names []string) {
	sort.Slice(names, func(i, j int) bool { return sequenceOf(names[i]) < sequenceOf(names[j]) })
}
