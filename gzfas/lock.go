package gzfas

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/rahelmartim/gzfas/coord"
)

// Mode is the kind of a lock.
type Mode string

const (
	Read  Mode = "read"
	Write Mode = "write"
)

// LockState is the state of a lock request.
type LockState string

const (
	LockWaiting  LockState = "waiting"
	LockHeld     LockState = "held"
	LockReleased LockState = "released"
	LockFailed   LockState = "failed"
)

// Lock is a readers-writer lock on one file, with the shared lock recipe of
// ZooKeeper:
//
//  1. Create an ephemeral sequential znode: locks/read- or locks/write-.
//  2. List the lock znodes and sort them by sequence number.
//  3. A writer holds the lock when its znode is the first one. A reader
//     holds it when no write znode comes before it.
//  4. Otherwise, watch only the znode that blocks you: for a writer, the
//     znode right before it. For a reader, the last write znode before it.
//     So a release wakes one client, not all of them.
//  5. When the watch fires, go back to step 2.
//
// Because the znodes are ephemeral, a client that crashes releases its locks.
//
// GZFAS adds MAX_READ: a reader also waits while MAX_READ earlier readers
// hold the file. Then it watches the list of lock znodes.
type Lock struct {
	client *Client
	File   string
	Mode   Mode
	Node   string // full path of the lock znode

	mu       sync.Mutex
	state    LockState
	watching string // name of the znode or folder that this lock watches
	err      error
	held     chan struct{}
	done     chan struct{}
}

// RequestLock creates the lock znode and starts to wait in the background.
// Use Wait to block until the lock is held.
func (c *Client) RequestLock(file string, mode Mode) (*Lock, error) {
	if err := ValidateName(file); err != nil {
		return nil, err
	}
	if mode != Read && mode != Write {
		return nil, errors.New("mode must be read or write")
	}
	if _, err := c.system.Leader(); err != nil {
		return nil, err
	}
	if ok, _, err := c.session.Exists(filePath(file)); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrNoFile
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrNoClient
	}
	if _, ok := c.locks[file]; ok {
		c.mu.Unlock()
		return nil, ErrAlreadyLocked
	}
	l := &Lock{client: c, File: file, Mode: mode, state: LockWaiting, held: make(chan struct{}), done: make(chan struct{})}
	c.locks[file] = l
	c.mu.Unlock()

	node, err := c.session.Create(coord.Join(locksPath(file), string(mode)+"-"), nil, coord.EphemeralSequential)
	if err != nil {
		c.forget(file, l)
		return nil, err
	}
	l.Node = node
	go l.run()
	return l, nil
}

// State returns the state of the lock.
func (l *Lock) State() LockState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Watching returns the znode or folder that a waiting lock watches.
func (l *Lock) Watching() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watching
}

// Err returns why a lock failed.
func (l *Lock) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Name returns the name of the lock znode, for example read-0000000003.
func (l *Lock) Name() string { return coord.Base(l.Node) }

// Wait blocks until the lock is held, fails, or ctx ends.
func (l *Lock) Wait(ctx context.Context) error {
	select {
	case <-l.held:
		return nil
	case <-l.done:
		if err := l.Err(); err != nil {
			return err
		}
		return errors.New("lock ended before it was held")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release deletes the lock znode. A waiting request is cancelled.
func (l *Lock) Release() error {
	l.mu.Lock()
	if l.state == LockReleased || l.state == LockFailed {
		l.mu.Unlock()
		return nil
	}
	l.state = LockReleased
	l.mu.Unlock()
	err := l.client.session.Delete(l.Node, coord.AnyVersion)
	if errors.Is(err, coord.ErrNoNode) {
		err = nil
	}
	l.client.forget(l.File, l)
	l.client.status()
	return err
}

func (l *Lock) setWatching(name string) {
	l.mu.Lock()
	l.watching = name
	l.mu.Unlock()
}

func (l *Lock) fail(err error) {
	l.mu.Lock()
	if l.state == LockWaiting || l.state == LockHeld {
		l.state, l.err = LockFailed, err
	}
	l.mu.Unlock()
	l.client.forget(l.File, l)
}

// run is the waiting loop of the recipe.
func (l *Lock) run() {
	defer close(l.done)
	session := l.client.session
	me := l.Name()
	l.client.status()
	for {
		if state := l.State(); state != LockWaiting {
			return
		}
		names, _, err := session.Children(locksPath(l.File))
		if err != nil {
			l.fail(lockError(err))
			return
		}
		sortBySequence(names)
		index := -1
		for i, name := range names {
			if name == me {
				index = i
			}
		}
		if index < 0 {
			l.fail(errors.New("the lock znode disappeared"))
			return
		}

		blocker, watchFolder, err := l.blocker(names[:index])
		if err != nil {
			l.fail(err)
			return
		}
		if blocker == "" && !watchFolder {
			l.mu.Lock()
			if l.state == LockWaiting {
				l.state, l.watching = LockHeld, ""
				close(l.held)
			}
			l.mu.Unlock()
			l.client.status()
			<-l.untilEnd()
			return
		}

		var events <-chan coord.Event
		if watchFolder {
			l.setWatching("locks/")
			_, _, events, err = session.ChildrenW(locksPath(l.File))
		} else {
			l.setWatching(blocker)
			var exists bool
			exists, _, events, err = session.ExistsW(coord.Join(locksPath(l.File), blocker))
			if err == nil && !exists {
				continue // It went away before the watch: check again now.
			}
		}
		if err != nil {
			l.fail(lockError(err))
			return
		}
		event := <-events
		if event.Type == coord.SessionClosed {
			l.fail(errors.New("the session ended"))
			return
		}
	}
}

// blocker returns the znode that this lock must wait for, or watchFolder
// when it waits for a free MAX_READ place.
func (l *Lock) blocker(before []string) (string, bool, error) {
	if l.Mode == Write {
		if len(before) == 0 {
			return "", false, nil
		}
		return before[len(before)-1], false, nil
	}
	for i := len(before) - 1; i >= 0; i-- {
		if strings.HasPrefix(before[i], string(Write)+"-") {
			return before[i], false, nil
		}
	}
	maxRead, err := l.client.system.confValue("MAX_READ")
	if err != nil {
		return "", false, err
	}
	if len(before) >= maxRead {
		return "", true, nil
	}
	return "", false, nil
}

// untilEnd returns a channel that closes when a held lock ends.
func (l *Lock) untilEnd() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			exists, _, events, err := l.client.session.ExistsW(l.Node)
			if err != nil || !exists {
				if l.State() == LockHeld {
					l.fail(errors.New("the lock znode was deleted"))
				}
				return
			}
			if event := <-events; event.Type == coord.NodeDeleted || event.Type == coord.SessionClosed {
				if l.State() == LockHeld {
					l.fail(errors.New("the session ended or the file was deleted"))
				}
				return
			}
		}
	}()
	return ch
}

func lockError(err error) error {
	if errors.Is(err, coord.ErrNoNode) {
		return ErrNoFile
	}
	if errors.Is(err, coord.ErrSessionClosed) {
		return errors.New("the session ended")
	}
	return err
}
