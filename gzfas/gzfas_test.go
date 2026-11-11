package gzfas

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rahelmartim/gzfas/coord"
)

// systems returns a fresh system on every store to test. With
// ZOOKEEPER_SERVERS set, the tests also run on that ZooKeeper, and they
// delete its /GZFAS tree: use a ZooKeeper for tests only.
func systems(t *testing.T) map[string]func(t *testing.T) *System {
	all := map[string]func(t *testing.T) *System{
		"memory": func(t *testing.T) *System { return newSystem(t, coord.NewMemoryStore()) },
	}
	if servers := os.Getenv("ZOOKEEPER_SERVERS"); servers != "" {
		all["zookeeper"] = func(t *testing.T) *System {
			store := &coord.ZooKeeperStore{Servers: strings.Split(servers, ","), Timeout: 10 * time.Second}
			clean, err := store.Connect("cleanup")
			if err != nil {
				t.Fatal(err)
			}
			_ = coord.DeleteTree(clean, Root)
			_ = clean.Close()
			return newSystem(t, store)
		}
	}
	return all
}

func newSystem(t *testing.T, store coord.Store) *System {
	t.Helper()
	s, err := New(store, nil, Config{MaxConn: 6, MaxRead: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.StartServer("s1", "10.0.0.1:7000"); err != nil {
		t.Fatal(err)
	}
	return s
}

func each(t *testing.T, fn func(t *testing.T, s *System)) {
	for name, make := range systems(t) {
		t.Run(name, func(t *testing.T) { fn(t, make(t)) })
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func client(t *testing.T, s *System, name string) *Client {
	t.Helper()
	c, err := s.Connect(name)
	must(t, err)
	return c
}

func request(t *testing.T, c *Client, file string, mode Mode) *Lock {
	t.Helper()
	l, err := c.RequestLock(file, mode)
	must(t, err)
	return l
}

// eventually waits until fn is true.
func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}

// stays checks that fn stays true for a short time.
func stays(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !fn() {
			t.Fatalf("changed too early: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func held(l *Lock) func() bool    { return func() bool { return l.State() == LockHeld } }
func waiting(l *Lock) func() bool { return func() bool { return l.State() == LockWaiting } }

func withFile(t *testing.T, s *System, file, content string) *Client {
	t.Helper()
	owner := client(t, s, "owner")
	must(t, owner.CreateFile(file, []byte(content)))
	return owner
}

func TestBootstrapCreatesTheTree(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		for _, path := range []string{ConfPath + "/MAX_CONN", ConfPath + "/MAX_READ", ServersPath, ClientsPath, DataPath, LeaderPath} {
			if ok, _, err := s.admin.Exists(path); err != nil || !ok {
				t.Errorf("%s is missing", path)
			}
		}
		// A second start on the same ZooKeeper keeps the configuration.
		must(t, s.SetConf("MAX_READ", 5))
		again, err := New(s.store, nil, Config{MaxRead: 1})
		must(t, err)
		defer again.Close()
		if n, _ := again.confValue("MAX_READ"); n != 5 {
			t.Fatalf("MAX_READ after restart = %d", n)
		}
	})
}

func TestClientsAreEphemeral(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		ana := client(t, s, "ana")
		path := coord.Join(ClientsPath, "ana")
		_, stat, err := s.admin.Get(path)
		must(t, err)
		if stat.EphemeralOwner != ana.SessionID() {
			t.Fatalf("owner = %q", stat.EphemeralOwner)
		}
		if _, err := s.Connect("ana"); !errors.Is(err, ErrNameTaken) {
			t.Fatalf("same name: %v", err)
		}
		must(t, ana.Disconnect())
		if ok, _, _ := s.admin.Exists(path); ok {
			t.Fatal("client znode survived the disconnect")
		}
		if _, err := s.Client("ana"); !errors.Is(err, ErrNoClient) {
			t.Fatal("client is still listed")
		}
	})
}

func TestMaxConn(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		must(t, s.SetConf("MAX_CONN", 2))
		client(t, s, "a")
		b := client(t, s, "b")
		if _, err := s.Connect("c"); !errors.Is(err, ErrTooManyConn) {
			t.Fatalf("third client: %v", err)
		}
		if ok, _, _ := s.admin.Exists(coord.Join(ClientsPath, "c")); ok {
			t.Fatal("rejected client left its znode")
		}
		must(t, b.Disconnect())
		client(t, s, "c")
	})
}

func TestReadersShareAndWritersWait(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia, caio := client(t, s, "ana"), client(t, s, "bia"), client(t, s, "caio")
		r1 := request(t, ana, "book.txt", Read)
		r2 := request(t, bia, "book.txt", Read)
		eventually(t, "both readers hold the lock", func() bool { return held(r1)() && held(r2)() })

		w := request(t, caio, "book.txt", Write)
		stays(t, "the writer waits for the readers", waiting(w))
		if w.Watching() != r2.Name() {
			t.Fatalf("writer watches %q, want the znode right before it (%q)", w.Watching(), r2.Name())
		}
		must(t, r2.Release())
		stays(t, "the writer still waits for the first reader", waiting(w))
		must(t, r1.Release())
		eventually(t, "the writer holds the lock", held(w))
	})
}

func TestWriterIsExclusiveAndReadersQueueBehindIt(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia, caio := client(t, s, "ana"), client(t, s, "bia"), client(t, s, "caio")
		w := request(t, ana, "book.txt", Write)
		eventually(t, "writer holds", held(w))
		r := request(t, bia, "book.txt", Read)
		w2 := request(t, caio, "book.txt", Write)
		stays(t, "the reader waits for the writer", waiting(r))
		if r.Watching() != w.Name() {
			t.Fatalf("reader watches %q, want the last writer before it (%q)", r.Watching(), w.Name())
		}
		if w2.Watching() != r.Name() {
			t.Fatalf("second writer watches %q, want %q", w2.Watching(), r.Name())
		}
		must(t, w.Release())
		eventually(t, "the reader holds", held(r))
		stays(t, "the second writer waits for the reader", waiting(w2))
		must(t, r.Release())
		eventually(t, "the second writer holds", held(w2))
	})
}

func TestNewReadersDoNotOvertakeAWaitingWriter(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia, caio := client(t, s, "ana"), client(t, s, "bia"), client(t, s, "caio")
		r1 := request(t, ana, "book.txt", Read)
		eventually(t, "first reader holds", held(r1))
		w := request(t, bia, "book.txt", Write)
		r2 := request(t, caio, "book.txt", Read)
		stays(t, "a later reader waits behind the writer", waiting(r2))
		if r2.Watching() != w.Name() {
			t.Fatalf("later reader watches %q, want the writer %q", r2.Watching(), w.Name())
		}
	})
}

func TestMaxRead(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1") // MAX_READ is 2 in these tests
		a, b, c := client(t, s, "a"), client(t, s, "b"), client(t, s, "c")
		r1, r2, r3 := request(t, a, "book.txt", Read), request(t, b, "book.txt", Read), request(t, c, "book.txt", Read)
		eventually(t, "two readers hold", func() bool { return held(r1)() && held(r2)() })
		stays(t, "the third reader waits for a free place", waiting(r3))
		if r3.Watching() != "locks/" {
			t.Fatalf("third reader watches %q, want the lock folder", r3.Watching())
		}
		must(t, r2.Release())
		eventually(t, "the third reader holds", held(r3))
	})
}

func TestDisconnectReleasesLocks(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia := client(t, s, "ana"), client(t, s, "bia")
		w := request(t, ana, "book.txt", Write)
		eventually(t, "ana holds", held(w))
		r := request(t, bia, "book.txt", Read)
		stays(t, "bia waits", waiting(r))
		must(t, ana.Disconnect()) // like a crash: the session ends
		eventually(t, "bia gets the lock when ana's ephemeral znode goes", held(r))
		if ok, _, _ := s.admin.Exists(w.Node); ok {
			t.Fatal("the lock znode of the crashed client survived")
		}
	})
}

func TestOneRequestPerFileAndRelease(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana := client(t, s, "ana")
		l := request(t, ana, "book.txt", Read)
		if _, err := ana.RequestLock("book.txt", Write); !errors.Is(err, ErrAlreadyLocked) {
			t.Fatalf("second request: %v", err)
		}
		eventually(t, "held", held(l))
		must(t, l.Release())
		must(t, l.Release()) // twice is fine
		if l.State() != LockReleased {
			t.Fatalf("state = %s", l.State())
		}
		if ok, _, _ := s.admin.Exists(l.Node); ok {
			t.Fatal("the lock znode survived the release")
		}
		request(t, ana, "book.txt", Write) // a new request works again
	})
}

func TestCancelAWaitingRequest(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia := client(t, s, "ana"), client(t, s, "bia")
		w := request(t, ana, "book.txt", Write)
		eventually(t, "held", held(w))
		r := request(t, bia, "book.txt", Read)
		must(t, r.Release())
		must(t, w.Release())
		w2 := request(t, ana, "book.txt", Write)
		eventually(t, "the cancelled reader does not block", held(w2))
	})
}

func TestWriteNeedsTheWriteLock(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana := client(t, s, "ana")
		if err := ana.Write("book.txt", []byte("x"), ""); !errors.Is(err, ErrNotLocked) {
			t.Fatalf("write without a lock: %v", err)
		}
		r := request(t, ana, "book.txt", Read)
		eventually(t, "held", held(r))
		if err := ana.Write("book.txt", []byte("x"), ""); !errors.Is(err, ErrNotLocked) {
			t.Fatalf("write with a read lock: %v", err)
		}
		content, _, err := ana.Read("book.txt")
		must(t, err)
		if string(content) != "v1" {
			t.Fatalf("read %q", content)
		}
	})
}

func TestVersionsAndRollback(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "first")
		ana := client(t, s, "ana")
		w := request(t, ana, "book.txt", Write)
		must(t, w.Wait(context.Background()))
		must(t, ana.Write("book.txt", []byte("second"), "fix typo"))
		must(t, ana.Write("book.txt", []byte("third"), ""))
		versions, err := s.Versions("book.txt")
		must(t, err)
		if len(versions) != 3 || versions[1].Note != "fix typo" || versions[2].Author != "ana" {
			t.Fatalf("versions = %+v", versions)
		}
		must(t, ana.Rollback("book.txt", versions[0].Name))
		content, meta, err := ana.Read("book.txt")
		must(t, err)
		if string(content) != "first" || meta.Versions != 4 || meta.SHA256 != digest([]byte("first")) {
			t.Fatalf("after rollback: %q %+v", content, meta)
		}
		if err := ana.Rollback("book.txt", "v-9999999999"); err == nil {
			t.Fatal("rollback to an unknown version worked")
		}
		if err := ana.Rollback("book.txt", "../../etc"); err == nil {
			t.Fatal("rollback accepted a bad version name")
		}
	})
}

func TestDeleteFileWakesWaitersWithAnError(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana, bia := client(t, s, "ana"), client(t, s, "bia")
		w := request(t, ana, "book.txt", Write)
		eventually(t, "held", held(w))
		r := request(t, bia, "book.txt", Read)
		stays(t, "waits", waiting(r))
		must(t, ana.DeleteFile("book.txt"))
		eventually(t, "the waiting reader fails", func() bool { return r.State() == LockFailed })
		if ok, _, _ := s.admin.Exists(filePath("book.txt")); ok {
			t.Fatal("file znode survived")
		}
		if _, err := s.blobs.Get("book.txt/" + "v-0000000000"); !errors.Is(err, ErrNoBlob) {
			t.Fatal("content survived")
		}
		if _, err := bia.RequestLock("book.txt", Read); !errors.Is(err, ErrNoFile) {
			t.Fatalf("lock on a deleted file: %v", err)
		}
	})
}

func TestFileValidation(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		ana := client(t, s, "ana")
		for _, bad := range []string{"", "../x", "a/b", ".hidden", strings.Repeat("a", 49), "white space"} {
			if err := ana.CreateFile(bad, nil); !errors.Is(err, ErrBadName) {
				t.Errorf("CreateFile(%q) = %v", bad, err)
			}
		}
		if err := ana.CreateFile("big.bin", make([]byte, MaxContentBytes+1)); !errors.Is(err, ErrTooLarge) {
			t.Errorf("large file: %v", err)
		}
		must(t, ana.CreateFile("ok.txt", nil))
		if err := ana.CreateFile("ok.txt", nil); !errors.Is(err, ErrFileExists) {
			t.Errorf("same file twice: %v", err)
		}
		if _, err := s.Connect("bad name"); !errors.Is(err, ErrBadName) {
			t.Errorf("bad client name: %v", err)
		}
		if err := s.SetConf("MAX_READ", 0); !errors.Is(err, ErrBadConf) {
			t.Errorf("MAX_READ 0: %v", err)
		}
		if err := s.SetConf("OTHER", 3); !errors.Is(err, ErrBadConf) {
			t.Errorf("unknown key: %v", err)
		}
	})
}

func TestLeaderElectionAndFailover(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		s1, _ := s.Server("s1")
		s2, err := s.StartServer("s2", "10.0.0.2:7000")
		must(t, err)
		s3, err := s.StartServer("s3", "10.0.0.3:7000")
		must(t, err)
		leader, err := s.Leader()
		must(t, err)
		if leader.ID != "s1" || !s1.IsLeader() || s2.IsLeader() {
			t.Fatalf("leader = %+v", leader)
		}
		if s2.Watching() != coord.Base(s1.Node) || s3.Watching() != coord.Base(s2.Node) {
			t.Fatalf("watch chain: s2 -> %q, s3 -> %q", s2.Watching(), s3.Watching())
		}
		// s2 stops: s3 now watches s1, and nobody else changes.
		s2.Stop()
		eventually(t, "s3 watches s1", func() bool { return s3.Watching() == coord.Base(s1.Node) })
		s1.Stop()
		eventually(t, "s3 becomes the leader", s3.IsLeader)
		leader, err = s.Leader()
		must(t, err)
		if leader.ID != "s3" {
			t.Fatalf("leader after failover = %+v", leader)
		}
	})
}

func TestNoLeaderNoService(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "v1")
		ana := client(t, s, "ana")
		s1, _ := s.Server("s1")
		s1.Stop()
		eventually(t, "leader znode goes", func() bool { _, err := s.Leader(); return errors.Is(err, ErrNoLeader) })
		if _, err := ana.RequestLock("book.txt", Read); !errors.Is(err, ErrNoLeader) {
			t.Fatalf("lock without a leader: %v", err)
		}
		_, err := s.StartServer("s4", "10.0.0.4:7000")
		must(t, err)
		request(t, ana, "book.txt", Read)
	})
}

func TestSnapshot(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		withFile(t, s, "book.txt", "hello")
		ana, bia := client(t, s, "ana"), client(t, s, "bia")
		w := request(t, ana, "book.txt", Write)
		eventually(t, "held", held(w))
		request(t, bia, "book.txt", Read)
		inspect, err := s.Inspector()
		must(t, err)
		defer inspect.Close()
		eventually(t, "the snapshot shows the queue", func() bool {
			snap, err := s.Snapshot(inspect)
			if err != nil || len(snap.Files) != 1 {
				return false
			}
			f := snap.Files[0]
			return f.Status == "write" && len(f.Queue) == 2 && f.Queue[0].Owner == "client ana" &&
				f.Queue[1].State == "waiting" && f.Queue[1].Watching == w.Name() && f.Content == "hello"
		})
		snap, _ := s.Snapshot(inspect)
		if snap.Leader == nil || snap.Leader.ID != "s1" || snap.Conf["MAX_READ"] != 2 || len(snap.Clients) != 3 {
			t.Fatalf("snapshot = %+v", snap)
		}
		found := false
		var walk func(n TreeNode)
		walk = func(n TreeNode) {
			if n.Path == coord.Join(ClientsPath, "bia") && n.Ephemeral && n.Owner == "client bia" {
				found = true
			}
			for _, c := range n.Children {
				walk(c)
			}
		}
		walk(snap.Tree)
		if !found {
			t.Fatal("the tree does not show bia's ephemeral znode")
		}
	})
}

func TestManyClientsKeepTheLockRules(t *testing.T) {
	each(t, func(t *testing.T, s *System) {
		must(t, s.SetConf("MAX_CONN", 30))
		must(t, s.SetConf("MAX_READ", 99))
		withFile(t, s, "counter", "0")
		var mu sync.Mutex
		readers, writers, broken := 0, 0, false
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			c := client(t, s, fmt.Sprintf("c%d", i))
			mode := Read
			if i%3 == 0 {
				mode = Write
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for round := 0; round < 3; round++ {
					l, err := c.RequestLock("counter", mode)
					if err != nil {
						t.Error(err)
						return
					}
					if err := l.Wait(context.Background()); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					if mode == Write {
						writers++
					} else {
						readers++
					}
					if writers > 1 || (writers == 1 && readers > 0) {
						broken = true
					}
					mu.Unlock()
					time.Sleep(2 * time.Millisecond)
					mu.Lock()
					if mode == Write {
						writers--
					} else {
						readers--
					}
					mu.Unlock()
					_ = l.Release()
				}
			}()
		}
		wg.Wait()
		if broken {
			t.Fatal("a writer shared the file with someone")
		}
	})
}

type failingBlobs struct{ MemoryBlobs }

func (*failingBlobs) Put(string, []byte) error { return errors.New("disk full") }

func TestFailedContentLeavesNoVersion(t *testing.T) {
	store := coord.NewMemoryStore()
	s, err := New(store, &failingBlobs{MemoryBlobs: *NewMemoryBlobs()}, Config{})
	must(t, err)
	defer s.Close()
	_, err = s.StartServer("s1", "x")
	must(t, err)
	ana := client(t, s, "ana")
	if err := ana.CreateFile("book.txt", []byte("x")); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("create with a failing blob store: %v", err)
	}
	versions, err := s.Versions("book.txt")
	must(t, err)
	if len(versions) != 0 {
		t.Fatalf("a version without content stayed: %+v", versions)
	}
}
