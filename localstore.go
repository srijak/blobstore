package blobstore

import (
	"os"
	"io"
	"fmt"
	"path/filepath"
	"strings"
	//	"io/ioutil"
	l4g "log4go.googlecode.com/hg"
)

type ILocalStore interface {
	Get(name string, vn IVnode, w io.Writer) (written int64, err os.Error)
	Put(name string, vn IVnode, r io.Reader) (written int64, err os.Error)
}

type DiskStore struct {
	rootDir string
}

func NewDiskStore(rootDir string) *DiskStore {
	d := &DiskStore{rootDir: rootDir}
	return d
}
func (ds *DiskStore) Put(name string, vn IVnode, r io.Reader) (written int64, err os.Error) {
	written = 0
	dir_path, full_path, err := ds.buildBlobPath(vn, name)
	if err != nil {
		return
	}

	err = os.MkdirAll(dir_path, 0700)
	if err != nil {
		return
	}

	_, err = os.Stat(full_path)
	if err == nil {
		// blob with same name exists.
		// means we are done.
		// TODO: check to verify size of the blobs are same
		//       and log that we tried to add existing blob.
		l4g.Info("Blob already exists at %s", full_path)
		return
	}
	f, err := os.Create(full_path)
	defer f.Close()
	if err != nil {
		return
	}

	written, err = io.Copy(f, r)
	return
}

func (ds *DiskStore) Get(name string, vn IVnode, w io.Writer) (written int64, err os.Error) {
	written = 0
	_, full_path, err := ds.buildBlobPath(vn, name)
	if err != nil {
		return
	}
	f, err := os.Open(full_path)
	defer f.Close()
	if err != nil {
		return
	}
	written, err = io.Copy(w, f)
	return
}

func (ds *DiskStore) buildBlobPath(vn IVnode, name string) (dir, file string, err os.Error) {
	// ignores the 4th char (assumes it the separator between algo name and
	// hash
	if len(name) < 11 {
		panic("DiskStore doesn't support storing data with name shorter than 11 chars")
	}

	vnode_dir := fmt.Sprintf("%d", vn.GetOffset())
	directory := filepath.Clean(filepath.Join(ds.rootDir, vnode_dir, name[0:4], name[5:8], name[8:11]))
	full_path := filepath.Clean(filepath.Join(directory, name))

	if !strings.HasPrefix(full_path, ds.rootDir) {
		msg := fmt.Sprintf("[%s] doesnt have prefix: [%s]", full_path, ds.rootDir)
		return "", "INVALID", os.NewError(msg)
	}

	return directory, full_path, nil
}
