package gzfas

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/rahelmartim/gzfas/coord"
)

// Snapshot is the whole state of the system, for the dashboard.
type Snapshot struct {
	Tree    TreeNode       `json:"tree"`
	Leader  *LeaderInfo    `json:"leader"`
	Servers []ServerView   `json:"servers"`
	Clients []ClientView   `json:"clients"`
	Files   []FileView     `json:"files"`
	Conf    map[string]int `json:"conf"`
}

// TreeNode is one znode of the tree.
type TreeNode struct {
	Path      string     `json:"path"`
	Name      string     `json:"name"`
	Data      string     `json:"data"`
	Version   int32      `json:"version"`
	Ephemeral bool       `json:"ephemeral"`
	Owner     string     `json:"owner"` // actor that owns an ephemeral znode
	Children  []TreeNode `json:"children"`
}

type ServerView struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Session  string `json:"session"`
	Node     string `json:"node"`
	Leader   bool   `json:"leader"`
	Watching string `json:"watching"`
}

type LockView struct {
	File     string    `json:"file"`
	Mode     Mode      `json:"mode"`
	Node     string    `json:"node"`
	State    LockState `json:"state"`
	Watching string    `json:"watching"`
	Error    string    `json:"error,omitempty"`
}

type ClientView struct {
	Name    string     `json:"name"`
	Session string     `json:"session"`
	Status  string     `json:"status"`
	Actions int        `json:"actions"`
	Locks   []LockView `json:"locks"`
}

// QueueEntry is one znode of the lock queue of a file.
type QueueEntry struct {
	Node     string `json:"node"`
	Mode     Mode   `json:"mode"`
	Owner    string `json:"owner"`
	State    string `json:"state"` // held, waiting, or unknown (owned by another process)
	Watching string `json:"watching"`
}

type FileView struct {
	Name     string       `json:"name"`
	Status   string       `json:"status"` // free, read, write
	Meta     FileMeta     `json:"meta"`
	Content  string       `json:"content"`
	Queue    []QueueEntry `json:"queue"`
	Versions []Version    `json:"versions"`
}

// Snapshot reads the state. It uses the given session, so that the reads
// do not appear in the call log when the session is not recorded.
func (s *System) Snapshot(inspect coord.Session) (Snapshot, error) {
	if inspect == nil {
		inspect = s.admin
	}
	// Empty lists, not nil, so that the JSON has [] instead of null.
	snap := Snapshot{Conf: map[string]int{}, Servers: []ServerView{}, Clients: []ClientView{}, Files: []FileView{}}
	tree, err := s.tree(inspect, Root)
	if err != nil {
		return snap, err
	}
	snap.Tree = tree
	for _, key := range []string{"MAX_CONN", "MAX_READ"} {
		if data, _, err := inspect.Get(coord.Join(ConfPath, key)); err == nil {
			var n int
			_ = json.Unmarshal(data, &n)
			snap.Conf[key] = n
		}
	}
	if data, _, err := inspect.Get(LeaderPath); err == nil {
		var info LeaderInfo
		if json.Unmarshal(data, &info) == nil {
			snap.Leader = &info
		}
	}

	for _, srv := range s.Servers() {
		snap.Servers = append(snap.Servers, ServerView{
			ID: srv.ID, Address: srv.Address, Session: srv.SessionID(), Node: coord.Base(srv.Node),
			Leader: srv.IsLeader(), Watching: srv.Watching(),
		})
	}

	ownLocks := map[string]*Lock{} // by full node path
	for _, c := range s.Clients() {
		view := ClientView{Name: c.Name, Session: c.SessionID(), Status: "connected", Locks: []LockView{}}
		if data, _, err := inspect.Get(coord.Join(ClientsPath, c.Name)); err == nil {
			var info ClientInfo
			if json.Unmarshal(data, &info) == nil {
				view.Status, view.Actions = info.Status, info.Actions
			}
		}
		for _, l := range c.Locks() {
			ownLocks[l.Node] = l
			lv := LockView{File: l.File, Mode: l.Mode, Node: l.Name(), State: l.State(), Watching: l.Watching()}
			if err := l.Err(); err != nil {
				lv.Error = err.Error()
			}
			view.Locks = append(view.Locks, lv)
		}
		snap.Clients = append(snap.Clients, view)
	}

	files, _, err := inspect.Children(DataPath)
	if err != nil {
		return snap, err
	}
	for _, name := range files {
		fv, err := s.fileView(inspect, name, ownLocks)
		if errors.Is(err, ErrNoFile) {
			continue // Deleted while we read it.
		}
		if err != nil {
			return snap, err
		}
		snap.Files = append(snap.Files, fv)
	}
	return snap, nil
}

func (s *System) fileView(inspect coord.Session, name string, own map[string]*Lock) (FileView, error) {
	fv := FileView{Name: name, Status: "free", Queue: []QueueEntry{}, Versions: []Version{}}
	content, meta, err := s.content(inspect, name)
	if err != nil && !errors.Is(err, ErrNoBlob) {
		return fv, err
	}
	fv.Meta = meta
	fv.Content = string(content)
	if len(fv.Content) > 4000 {
		fv.Content = fv.Content[:4000] + "…"
	}
	names, _, err := inspect.Children(locksPath(name))
	if errors.Is(err, coord.ErrNoNode) {
		return fv, ErrNoFile
	}
	if err != nil {
		return fv, err
	}
	sortBySequence(names)
	for _, node := range names {
		entry := QueueEntry{Node: node, Mode: Read, State: "unknown"}
		if strings.HasPrefix(node, string(Write)+"-") {
			entry.Mode = Write
		}
		_, stat, err := inspect.Get(coord.Join(locksPath(name), node))
		if err == nil {
			entry.Owner = s.Actor(stat.EphemeralOwner)
		}
		if l, ok := own[coord.Join(locksPath(name), node)]; ok {
			entry.State = string(l.State())
			entry.Watching = l.Watching()
			if entry.State == string(LockHeld) {
				fv.Status = string(l.Mode)
			}
		}
		fv.Queue = append(fv.Queue, entry)
	}
	fv.Versions, _ = s.versions(inspect, name)
	return fv, nil
}

func (s *System) tree(inspect coord.Session, path string) (TreeNode, error) {
	data, stat, err := inspect.Get(path)
	if err != nil {
		return TreeNode{}, err
	}
	node := TreeNode{
		Path: path, Name: coord.Base(path), Data: string(data), Version: stat.Version,
		Ephemeral: stat.EphemeralOwner != "", Children: []TreeNode{},
	}
	if path == "/" {
		node.Name = "/"
	}
	if node.Ephemeral {
		node.Owner = s.Actor(stat.EphemeralOwner)
	}
	if len(node.Data) > 160 {
		node.Data = node.Data[:160] + "…"
	}
	children, _, err := inspect.Children(path)
	if err != nil {
		return node, err
	}
	sortBySequence(children)
	if !strings.HasSuffix(path, "/locks") && !strings.HasSuffix(path, "/versions") && path != ServersPath {
		// Sequential children keep their order. The other children are sorted by name.
		sortNames(children)
	}
	for _, child := range children {
		c, err := s.tree(inspect, coord.Join(path, child))
		if errors.Is(err, coord.ErrNoNode) {
			continue
		}
		if err != nil {
			return node, err
		}
		node.Children = append(node.Children, c)
	}
	return node, nil
}

func sortNames(names []string) {
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
}

// Inspector returns a session for snapshots. It is not recorded, so the
// dashboard reads do not fill the call log.
func (s *System) Inspector() (coord.Session, error) {
	store := s.store
	if rec, ok := store.(*coord.Recorder); ok {
		store = rec.Store()
	}
	session, err := store.Connect("dashboard")
	if err != nil {
		return nil, err
	}
	s.remember(session, "dashboard")
	return session, nil
}
