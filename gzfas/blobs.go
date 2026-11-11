package gzfas

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

// BlobStore keeps the content of each file version. ZooKeeper only keeps
// the metadata, because a znode holds at most about 1 MB.
type BlobStore interface {
	Put(key string, data []byte) error
	Get(key string) ([]byte, error)
	Delete(prefix string) error
}

// ErrNoBlob is returned for a missing blob.
var ErrNoBlob = errors.New("content not found")

// MemoryBlobs keeps blobs in memory.
type MemoryBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMemoryBlobs() *MemoryBlobs { return &MemoryBlobs{data: map[string][]byte{}} }

func (m *MemoryBlobs) Put(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemoryBlobs) Get(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[key]
	if !ok {
		return nil, ErrNoBlob
	}
	return append([]byte(nil), data...), nil
}

func (m *MemoryBlobs) Delete(prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(m.data, key)
		}
	}
	return nil
}

// DirBlobs keeps blobs as files in a folder: <dir>/<file>/<version>.
type DirBlobs struct {
	Dir string
}

var blobKeyRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,47}(/v-[0-9]{10})?/?$`)

func (d DirBlobs) path(key string) (string, error) {
	// Keys come from validated names, but check again before using the disk.
	if !blobKeyRe.MatchString(key) {
		return "", ErrBadName
	}
	return filepath.Join(d.Dir, filepath.FromSlash(key)), nil
}

func (d DirBlobs) Put(key string, data []byte) error {
	path, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o640)
}

func (d DirBlobs) Get(key string) ([]byte, error) {
	path, err := d.path(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoBlob
	}
	return data, err
}

func (d DirBlobs) Delete(prefix string) error {
	path, err := d.path(prefix)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
