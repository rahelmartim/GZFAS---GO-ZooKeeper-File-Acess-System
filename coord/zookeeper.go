//go:build !js

package coord

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

// ZooKeeperStore opens real ZooKeeper sessions. Each Connect is a separate
// session, so ephemeral znodes and watches behave exactly as in production.
type ZooKeeperStore struct {
	Servers []string
	Timeout time.Duration
}

var _ Store = (*ZooKeeperStore)(nil)

type quietLogger struct{}

func (quietLogger) Printf(string, ...any) {}

func (z *ZooKeeperStore) Connect(name string) (Session, error) {
	timeout := z.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	conn, events, err := zk.Connect(z.Servers, timeout, zk.WithLogger(quietLogger{}))
	if err != nil {
		return nil, err
	}
	deadline := time.After(timeout)
	for {
		select {
		case event := <-events:
			if event.State == zk.StateHasSession {
				return &zkSession{conn: conn, id: fmt.Sprintf("0x%x", conn.SessionID())}, nil
			}
		case <-deadline:
			conn.Close()
			return nil, fmt.Errorf("connecting to ZooKeeper at %v: timed out", z.Servers)
		}
	}
}

type zkSession struct {
	conn *zk.Conn
	id   string
}

func (s *zkSession) ID() string { return s.id }

func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, zk.ErrNoNode):
		return ErrNoNode
	case errors.Is(err, zk.ErrNodeExists):
		return ErrNodeExists
	case errors.Is(err, zk.ErrNotEmpty):
		return ErrNotEmpty
	case errors.Is(err, zk.ErrBadVersion):
		return ErrBadVersion
	case errors.Is(err, zk.ErrNoChildrenForEphemerals):
		return ErrNoChildrenForEphemerals
	case errors.Is(err, zk.ErrClosing), errors.Is(err, zk.ErrConnectionClosed), errors.Is(err, zk.ErrSessionExpired):
		return ErrSessionClosed
	default:
		return err
	}
}

func stat(s *zk.Stat) Stat {
	if s == nil {
		return Stat{}
	}
	owner := ""
	if s.EphemeralOwner != 0 {
		owner = fmt.Sprintf("0x%x", s.EphemeralOwner)
	}
	return Stat{
		Version: s.Version, Cversion: s.Cversion, NumChildren: s.NumChildren, EphemeralOwner: owner,
		Czxid: s.Czxid, Mzxid: s.Mzxid, Ctime: time.UnixMilli(s.Ctime), Mtime: time.UnixMilli(s.Mtime),
	}
}

// events converts a ZooKeeper watch channel into a coord one.
func events(in <-chan zk.Event) <-chan Event {
	out := make(chan Event, 1)
	go func() {
		event, ok := <-in
		if !ok || event.Type == zk.EventNotWatching || event.Err != nil {
			out <- Event{Type: SessionClosed, Path: event.Path}
			return
		}
		types := map[zk.EventType]EventType{
			zk.EventNodeCreated: NodeCreated, zk.EventNodeDeleted: NodeDeleted,
			zk.EventNodeDataChanged: NodeDataChanged, zk.EventNodeChildrenChanged: NodeChildrenChanged,
		}
		out <- Event{Type: types[event.Type], Path: event.Path}
	}()
	return out
}

func (s *zkSession) Create(path string, data []byte, mode Mode) (string, error) {
	if err := ValidatePath(path); err != nil {
		return "", err
	}
	var flags int32
	if mode&Ephemeral != 0 {
		flags |= zk.FlagEphemeral
	}
	if mode&Sequential != 0 {
		flags |= zk.FlagSequence
	}
	created, err := s.conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
	return created, translate(err)
}

func (s *zkSession) Get(path string) ([]byte, Stat, error) {
	data, st, err := s.conn.Get(path)
	return data, stat(st), translate(err)
}

func (s *zkSession) GetW(path string) ([]byte, Stat, <-chan Event, error) {
	data, st, ch, err := s.conn.GetW(path)
	if err != nil {
		return nil, Stat{}, nil, translate(err)
	}
	return data, stat(st), events(ch), nil
}

func (s *zkSession) Exists(path string) (bool, Stat, error) {
	ok, st, err := s.conn.Exists(path)
	return ok, stat(st), translate(err)
}

func (s *zkSession) ExistsW(path string) (bool, Stat, <-chan Event, error) {
	ok, st, ch, err := s.conn.ExistsW(path)
	if err != nil {
		return false, Stat{}, nil, translate(err)
	}
	return ok, stat(st), events(ch), nil
}

func (s *zkSession) Children(path string) ([]string, Stat, error) {
	names, st, err := s.conn.Children(path)
	return names, stat(st), translate(err)
}

func (s *zkSession) ChildrenW(path string) ([]string, Stat, <-chan Event, error) {
	names, st, ch, err := s.conn.ChildrenW(path)
	if err != nil {
		return nil, Stat{}, nil, translate(err)
	}
	return names, stat(st), events(ch), nil
}

func (s *zkSession) Set(path string, data []byte, version int32) (Stat, error) {
	st, err := s.conn.Set(path, data, version)
	return stat(st), translate(err)
}

func (s *zkSession) Delete(path string, version int32) error {
	return translate(s.conn.Delete(path, version))
}

func (s *zkSession) Close() error {
	s.conn.Close()
	return nil
}
