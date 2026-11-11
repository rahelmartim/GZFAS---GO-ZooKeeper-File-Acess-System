// Package coord is the part of the ZooKeeper API that GZFAS uses: sessions,
// znodes with versions, ephemeral and sequential znodes, and one-time
// watches.
//
// Two stores implement it. MemoryStore follows the ZooKeeper rules in
// memory, for tests and for the browser demo. ZooKeeperStore talks to a
// real ZooKeeper server. A Recorder wraps either store and logs every call
// in zkCli syntax.
package coord

import (
	"errors"
	"strings"
	"time"
)

// Mode is how a znode is created.
type Mode int

const (
	// Persistent znodes stay until somebody deletes them.
	Persistent Mode = 0
	// Ephemeral znodes disappear when the session that created them ends.
	Ephemeral Mode = 1
	// Sequential znodes get a 10-digit counter after their name.
	Sequential Mode = 2
	// EphemeralSequential combines both. Lock and election recipes use it.
	EphemeralSequential = Ephemeral | Sequential
)

// AnyVersion makes Set and Delete ignore the version of the znode.
const AnyVersion int32 = -1

// Stat is the metadata of a znode.
type Stat struct {
	Version        int32  // number of data changes
	Cversion       int32  // number of child changes
	NumChildren    int32  //
	EphemeralOwner string // session that owns an ephemeral znode, or ""
	Czxid          int64  // transaction that created the znode
	Mzxid          int64  // transaction that changed the data last
	Ctime          time.Time
	Mtime          time.Time
}

// EventType is the kind of a watch event.
type EventType string

const (
	NodeCreated         EventType = "created"
	NodeDeleted         EventType = "deleted"
	NodeDataChanged     EventType = "data-changed"
	NodeChildrenChanged EventType = "children-changed"
	// SessionClosed ends every pending watch of a session that ended.
	SessionClosed EventType = "session-closed"
)

// Event is delivered once on a watch channel.
type Event struct {
	Type EventType
	Path string
}

// Errors, named like the ZooKeeper errors.
var (
	ErrNoNode                  = errors.New("node does not exist")
	ErrNodeExists              = errors.New("node already exists")
	ErrNotEmpty                = errors.New("node has children")
	ErrBadVersion              = errors.New("version does not match")
	ErrNoChildrenForEphemerals = errors.New("ephemeral nodes cannot have children")
	ErrSessionClosed           = errors.New("session is closed")
	ErrBadPath                 = errors.New("invalid path")
)

// Session is one connection to ZooKeeper. Its ephemeral znodes live as long
// as the session.
type Session interface {
	ID() string
	Create(path string, data []byte, mode Mode) (string, error)
	Get(path string) ([]byte, Stat, error)
	GetW(path string) ([]byte, Stat, <-chan Event, error)
	Exists(path string) (bool, Stat, error)
	ExistsW(path string) (bool, Stat, <-chan Event, error)
	Children(path string) ([]string, Stat, error)
	ChildrenW(path string) ([]string, Stat, <-chan Event, error)
	Set(path string, data []byte, version int32) (Stat, error)
	Delete(path string, version int32) error
	// Close ends the session. ZooKeeper then deletes its ephemeral znodes.
	Close() error
}

// Store opens sessions.
type Store interface {
	Connect(name string) (Session, error)
}

// ValidatePath reports an error unless path is an absolute znode path.
func ValidatePath(path string) error {
	if path == "/" {
		return nil
	}
	if !strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return ErrBadPath
	}
	for _, part := range strings.Split(path[1:], "/") {
		if part == "" || part == "." || part == ".." {
			return ErrBadPath
		}
	}
	return nil
}

// Join builds a znode path from parts.
func Join(parent string, names ...string) string {
	path := parent
	for _, name := range names {
		if path == "/" {
			path += name
		} else {
			path += "/" + name
		}
	}
	return path
}

// Parent returns the parent path of a valid path that is not the root.
func Parent(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "/"
	}
	return path[:i]
}

// Base returns the last part of a path.
func Base(path string) string {
	return path[strings.LastIndex(path, "/")+1:]
}

// EnsurePath creates the missing persistent znodes of a path.
func EnsurePath(s Session, path string) error {
	if err := ValidatePath(path); err != nil {
		return err
	}
	current := ""
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		current += "/" + part
		if _, err := s.Create(current, nil, Persistent); err != nil && !errors.Is(err, ErrNodeExists) {
			return err
		}
	}
	return nil
}

// DeleteTree deletes a znode and all of its children.
func DeleteTree(s Session, path string) error {
	children, _, err := s.Children(path)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := DeleteTree(s, Join(path, child)); err != nil && !errors.Is(err, ErrNoNode) {
			return err
		}
	}
	err = s.Delete(path, AnyVersion)
	if errors.Is(err, ErrNoNode) {
		return nil
	}
	return err
}
