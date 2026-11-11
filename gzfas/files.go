package gzfas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/rahelmartim/gzfas/coord"
)

// FileMeta is the data of /GZFAS/data/<file>.
type FileMeta struct {
	Current   string    `json:"current"` // name of the current version znode
	Versions  int       `json:"versions"`
	Size      int       `json:"size"`
	SHA256    string    `json:"sha256"`
	UpdatedBy string    `json:"updatedBy"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// VersionMeta is the data of /GZFAS/data/<file>/versions/v-<n>.
type VersionMeta struct {
	Author string    `json:"author"`
	Size   int       `json:"size"`
	SHA256 string    `json:"sha256"`
	At     time.Time `json:"at"`
	Note   string    `json:"note"`
}

var versionRe = regexp.MustCompile(`^v-[0-9]{10}$`)

// Version is one entry of the history of a file.
type Version struct {
	Name string `json:"name"`
	VersionMeta
}

func digest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func checkContent(content []byte, note string) error {
	if len(content) > MaxContentBytes {
		return ErrTooLarge
	}
	if len(note) > MaxNoteBytes {
		return fmt.Errorf("the note is longer than %d characters", MaxNoteBytes)
	}
	return nil
}

// CreateFile creates a file with its first version. It takes the write lock
// for that version, like every other write.
func (c *Client) CreateFile(file string, content []byte) error {
	if err := ValidateName(file); err != nil {
		return err
	}
	if err := checkContent(content, ""); err != nil {
		return err
	}
	if _, err := c.system.Leader(); err != nil {
		return err
	}
	meta, _ := json.Marshal(FileMeta{UpdatedBy: c.Name, UpdatedAt: c.system.now()})
	if _, err := c.session.Create(filePath(file), meta, coord.Persistent); err != nil {
		if errors.Is(err, coord.ErrNodeExists) {
			return ErrFileExists
		}
		return err
	}
	for _, path := range []string{locksPath(file), versionsPath(file)} {
		if _, err := c.session.Create(path, nil, coord.Persistent); err != nil {
			return err
		}
	}
	lock, err := c.RequestLock(file, Write)
	if err != nil {
		return err
	}
	defer lock.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lock.Wait(ctx); err != nil {
		return err
	}
	return c.Write(file, content, "created")
}

// heldLock returns the held lock of the client on a file, of the given mode
// or stronger.
func (c *Client) heldLock(file string, mode Mode) (*Lock, error) {
	l, ok := c.lockFor(file)
	if !ok || l.State() != LockHeld {
		if mode == Write {
			return nil, ErrNotLocked
		}
		return nil, errors.New("this needs a read or write lock of the file")
	}
	if mode == Write && l.Mode != Write {
		return nil, ErrNotLocked
	}
	return l, nil
}

// Read returns the current content of a file. It needs a read or write lock.
func (c *Client) Read(file string) ([]byte, FileMeta, error) {
	if _, err := c.heldLock(file, Read); err != nil {
		return nil, FileMeta{}, err
	}
	return c.system.content(c.session, file)
}

func (s *System) content(session coord.Session, file string) ([]byte, FileMeta, error) {
	data, _, err := session.Get(filePath(file))
	if errors.Is(err, coord.ErrNoNode) {
		return nil, FileMeta{}, ErrNoFile
	}
	if err != nil {
		return nil, FileMeta{}, err
	}
	var meta FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, FileMeta{}, err
	}
	if meta.Current == "" {
		return nil, meta, nil
	}
	content, err := s.blobs.Get(file + "/" + meta.Current)
	return content, meta, err
}

// Write saves a new version of a file. It needs the write lock.
//
// The new version is a sequential znode. The file znode is then updated
// with the version that was read, so a lost update is impossible even
// without the lock.
func (c *Client) Write(file string, content []byte, note string) error {
	if err := checkContent(content, note); err != nil {
		return err
	}
	if _, err := c.heldLock(file, Write); err != nil {
		return err
	}
	data, stat, err := c.session.Get(filePath(file))
	if err != nil {
		return lockError(err)
	}
	var meta FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	now := c.system.now()
	version := VersionMeta{Author: c.Name, Size: len(content), SHA256: digest(content), At: now, Note: note}
	vdata, _ := json.Marshal(version)
	node, err := c.session.Create(coord.Join(versionsPath(file), "v-"), vdata, coord.Sequential)
	if err != nil {
		return err
	}
	if err := c.system.blobs.Put(file+"/"+coord.Base(node), content); err != nil {
		// Without its content, the version must not exist.
		_ = c.session.Delete(node, coord.AnyVersion)
		return fmt.Errorf("saving the content: %w", err)
	}
	meta = FileMeta{
		Current: coord.Base(node), Versions: meta.Versions + 1, Size: len(content),
		SHA256: version.SHA256, UpdatedBy: c.Name, UpdatedAt: now,
	}
	next, _ := json.Marshal(meta)
	if _, err := c.session.Set(filePath(file), next, stat.Version); err != nil {
		return fmt.Errorf("another writer changed the file first: %w", err)
	}
	c.status()
	return nil
}

// Rollback saves an old version again as the newest one. It needs the write lock.
func (c *Client) Rollback(file, version string) error {
	if _, err := c.heldLock(file, Write); err != nil {
		return err
	}
	if !versionRe.MatchString(version) {
		return errors.New("unknown version")
	}
	if ok, _, err := c.session.Exists(coord.Join(versionsPath(file), version)); err != nil || !ok {
		return errors.New("unknown version")
	}
	content, err := c.system.blobs.Get(file + "/" + version)
	if err != nil {
		return err
	}
	return c.Write(file, content, "rollback to "+version)
}

// DeleteFile deletes a file with its history. It needs the write lock.
// The clients that wait for its lock get an error.
func (c *Client) DeleteFile(file string) error {
	l, err := c.heldLock(file, Write)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.state = LockReleased
	l.mu.Unlock()
	c.forget(file, l)
	if err := coord.DeleteTree(c.session, filePath(file)); err != nil {
		return err
	}
	c.status()
	return c.system.blobs.Delete(file + "/")
}

// Versions returns the history of a file, oldest first.
func (s *System) Versions(file string) ([]Version, error) {
	return s.versions(s.admin, file)
}

func (s *System) versions(session coord.Session, file string) ([]Version, error) {
	names, _, err := session.Children(versionsPath(file))
	if errors.Is(err, coord.ErrNoNode) {
		return nil, ErrNoFile
	}
	if err != nil {
		return nil, err
	}
	sortBySequence(names)
	out := make([]Version, 0, len(names))
	for _, name := range names {
		data, _, err := session.Get(coord.Join(versionsPath(file), name))
		if err != nil {
			continue
		}
		v := Version{Name: name}
		_ = json.Unmarshal(data, &v.VersionMeta)
		out = append(out, v)
	}
	return out, nil
}

// Files returns the file names.
func (s *System) Files() ([]string, error) {
	names, _, err := s.admin.Children(DataPath)
	sort.Strings(names)
	return names, err
}
