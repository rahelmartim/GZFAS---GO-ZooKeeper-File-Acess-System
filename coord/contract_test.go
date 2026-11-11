package coord

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// stores returns every store to test. A real ZooKeeper is used when
// ZOOKEEPER_SERVERS is set, for example to localhost:2181.
func stores(t *testing.T) map[string]Store {
	all := map[string]Store{"memory": NewMemoryStore()}
	if servers := os.Getenv("ZOOKEEPER_SERVERS"); servers != "" {
		all["zookeeper"] = &ZooKeeperStore{Servers: strings.Split(servers, ","), Timeout: 10 * time.Second}
	}
	return all
}

// root creates a fresh test folder, so that real ZooKeeper runs do not mix.
func root(t *testing.T, s Session) string {
	t.Helper()
	path := fmt.Sprintf("/gzfas-test-%d", time.Now().UnixNano())
	if _, err := s.Create(path, nil, Persistent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = DeleteTree(s, path) })
	return path
}

func connect(t *testing.T, store Store, name string) Session {
	t.Helper()
	s, err := store.Connect(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("no watch event")
		return Event{}
	}
}

func silent(t *testing.T, ch <-chan Event) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected event %+v", e)
	case <-time.After(150 * time.Millisecond):
	}
}

func each(t *testing.T, fn func(t *testing.T, store Store)) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) { fn(t, store) })
	}
}

func TestCreateGetSetDelete(t *testing.T) {
	each(t, func(t *testing.T, store Store) {
		s := connect(t, store, "a")
		base := root(t, s)
		path := base + "/file"
		created, err := s.Create(path, []byte("v1"), Persistent)
		must(t, err)
		if created != path {
			t.Fatalf("created %q, want %q", created, path)
		}
		if _, err := s.Create(path, nil, Persistent); !errors.Is(err, ErrNodeExists) {
			t.Fatalf("second create: %v", err)
		}
		data, stat, err := s.Get(path)
		must(t, err)
		if string(data) != "v1" || stat.Version != 0 {
			t.Fatalf("get = %q v%d", data, stat.Version)
		}
		stat, err = s.Set(path, []byte("v2"), 0)
		must(t, err)
		if stat.Version != 1 {
			t.Fatalf("version after set = %d", stat.Version)
		}
		if _, err := s.Set(path, []byte("v3"), 0); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("set with an old version: %v", err)
		}
		if err := s.Delete(path, 0); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("delete with an old version: %v", err)
		}
		must(t, s.Delete(path, 1))
		if _, _, err := s.Get(path); !errors.Is(err, ErrNoNode) {
			t.Fatalf("get after delete: %v", err)
		}
	})
}

func TestParentsAndChildren(t *testing.T) {
	each(t, func(t *testing.T, store Store) {
		s := connect(t, store, "a")
		base := root(t, s)
		if _, err := s.Create(base+"/missing/child", nil, Persistent); !errors.Is(err, ErrNoNode) {
			t.Fatalf("create without parent: %v", err)
		}
		must(t, EnsurePath(s, base+"/x/y/z"))
		must(t, EnsurePath(s, base+"/x/y/z")) // twice is fine
		if err := s.Delete(base+"/x", AnyVersion); !errors.Is(err, ErrNotEmpty) {
			t.Fatalf("delete with children: %v", err)
		}
		names, stat, err := s.Children(base + "/x")
		must(t, err)
		if len(names) != 1 || names[0] != "y" || stat.NumChildren != 1 {
			t.Fatalf("children = %v (%d)", names, stat.NumChildren)
		}
		must(t, DeleteTree(s, base+"/x"))
		if ok, _, _ := s.Exists(base + "/x"); ok {
			t.Fatal("DeleteTree left the znode")
		}
	})
}

func TestSequentialNodes(t *testing.T) {
	each(t, func(t *testing.T, store Store) {
		s := connect(t, store, "a")
		base := root(t, s)
		var names []string
		for i := 0; i < 3; i++ {
			created, err := s.Create(base+"/lock-", nil, Sequential)
			must(t, err)
			names = append(names, Base(created))
		}
		for i, name := range names {
			if want := fmt.Sprintf("lock-%010d", i); name != want {
				t.Fatalf("sequential name %d = %q, want %q", i, name, want)
			}
		}
		// The counter counts every child, so it never reuses a number.
		must(t, s.Delete(Join(base, names[2]), AnyVersion))
		created, err := s.Create(base+"/lock-", nil, Sequential)
		must(t, err)
		if Base(created) != "lock-0000000003" {
			t.Fatalf("after a delete: %q", Base(created))
		}
	})
}

func TestEphemeralNodesEndWithTheSession(t *testing.T) {
	each(t, func(t *testing.T, store Store) {
		admin := connect(t, store, "admin")
		base := root(t, admin)
		client, err := store.Connect("client")
		must(t, err)
		path, err := client.Create(base+"/client-", []byte("hello"), EphemeralSequential)
		must(t, err)
		_, stat, err := admin.Get(path)
		must(t, err)
		if stat.EphemeralOwner != client.ID() {
			t.Fatalf("owner = %q, want %q", stat.EphemeralOwner, client.ID())
		}
		if _, err := client.Create(path+"/child", nil, Persistent); !errors.Is(err, ErrNoChildrenForEphemerals) {
			t.Fatalf("child of an ephemeral znode: %v", err)
		}
		_, _, deleted, err := admin.ExistsW(path)
		must(t, err)
		must(t, client.Close())
		if e := receive(t, deleted); e.Type != NodeDeleted {
			t.Fatalf("event = %+v", e)
		}
		if ok, _, _ := admin.Exists(path); ok {
			t.Fatal("ephemeral znode survived its session")
		}
		if _, err := client.Create(base+"/again", nil, Persistent); err == nil {
			t.Fatal("a closed session still works")
		}
	})
}

func TestWatchesFireOnce(t *testing.T) {
	each(t, func(t *testing.T, store Store) {
		s := connect(t, store, "a")
		other := connect(t, store, "b")
		base := root(t, s)
		path := base + "/w"

		_, _, created, err := s.ExistsW(path)
		must(t, err)
		_, _, children, err := s.ChildrenW(base)
		must(t, err)
		_, err = other.Create(path, []byte("1"), Persistent)
		must(t, err)
		if e := receive(t, created); e.Type != NodeCreated || e.Path != path {
			t.Fatalf("exists watch: %+v", e)
		}
		if e := receive(t, children); e.Type != NodeChildrenChanged {
			t.Fatalf("children watch: %+v", e)
		}

		_, _, changed, err := s.GetW(path)
		must(t, err)
		_, err = other.Set(path, []byte("2"), AnyVersion)
		must(t, err)
		if e := receive(t, changed); e.Type != NodeDataChanged {
			t.Fatalf("data watch: %+v", e)
		}
		// The watch fired once: a second change sends nothing on it.
		_, err = other.Set(path, []byte("3"), AnyVersion)
		must(t, err)
		silent(t, changed)
	})
}

func TestValidation(t *testing.T) {
	for _, path := range []string{"", "a", "/a/", "//a", "/a/../b"} {
		if !errors.Is(ValidatePath(path), ErrBadPath) {
			t.Errorf("ValidatePath(%q) accepted", path)
		}
	}
	if Join("/", "a", "b") != "/a/b" || Parent("/a/b") != "/a" || Parent("/a") != "/" || Base("/a/b") != "b" {
		t.Error("path helpers")
	}
}

func TestMemorySessionCloseEndsItsWatches(t *testing.T) {
	store := NewMemoryStore()
	a, _ := store.Connect("a")
	_, _, ch, err := a.ExistsW("/nothing")
	must(t, err)
	if store.WatchCount() != 1 {
		t.Fatalf("watches = %d", store.WatchCount())
	}
	must(t, a.Close())
	if e := receive(t, ch); e.Type != SessionClosed {
		t.Fatalf("event = %+v", e)
	}
	if store.WatchCount() != 0 {
		t.Fatalf("watches after close = %d", store.WatchCount())
	}
}

func TestRecorderLogsZkCliCommands(t *testing.T) {
	rec := NewRecorder(NewMemoryStore(), 100)
	var seen []Call
	stop := rec.Subscribe(func(c Call) { seen = append(seen, c) })
	s, _ := rec.Connect("client ana")
	_, _ = s.Create("/a", []byte("x"), Persistent)
	_, _ = s.Create("/a/lock-", nil, EphemeralSequential)
	_, _, _, _ = s.ExistsW("/a/lock-0000000000")
	_ = s.Delete("/missing", 3)
	stop()
	_, _ = s.Create("/b", nil, Persistent)

	commands := []string{}
	for _, c := range rec.Calls() {
		commands = append(commands, c.Command)
	}
	want := []string{
		"connect  # client ana", "create /a 'x'", "create -e -s /a/lock-", "stat -w /a/lock-0000000000",
		"delete /missing -v 3", "create /b",
	}
	if strings.Join(commands, "|") != strings.Join(want, "|") {
		t.Fatalf("commands:\n%v\nwant:\n%v", commands, want)
	}
	if len(seen) != 5 {
		t.Fatalf("subscriber saw %d calls after stop", len(seen))
	}
	calls := rec.Calls()
	if calls[2].Result != "/a/lock-0000000000" || !calls[4].Error || calls[1].Actor != "client ana" {
		t.Fatalf("results: %+v", calls)
	}
}
