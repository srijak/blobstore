package blobstore

import (
	"os"
	"fmt"
	"path/filepath"
	"io/ioutil"
	l4g "log4go.googlecode.com/hg"
	"rpc"
)

type BlobStore struct {
	ks      *KeySpace
	rootDir string
}

func NewBlobStore() *BlobStore {
	b := new(BlobStore)
	b.ks = NewKeySpace("/blobstore.keyspace", "localhost:2181", 5e6) //TODO: configurable
	b.ks.Connect()
	b.rootDir = "/tmp/vnodes"
	return b
}

func (b *BlobStore) Get(hash *string, blob *[]byte) os.Error {
	resp_vnode, err := b.ks.GetResponsibleVnode(*hash)
	if err != nil {
		return err
	}

	if resp_vnode.isLocal() {
		l4g.Debug("Get. [%s] hash is local.", *hash)
		*blob, err = b.getBlob(resp_vnode.dir, *hash)
		if err != nil {
			return err
		}

	} else {

		l4g.Info("Making rpc call to retrieve from relevant remote server [%s]", resp_vnode.host_addr)
		return b.getRemoteBlob(hash, blob, resp_vnode.host_addr)
	}
	return nil
}

func (b *BlobStore) Put(blob *[]byte, hash *string) os.Error {
	*hash = getHash(blob)
	resp_vnode, err := b.ks.GetResponsibleVnode(*hash)
	if err != nil {
		return err
	}

	if resp_vnode.isLocal() {
		l4g.Info("Put. [%s] hash is local.", *hash)
		err := b.storeBlob(resp_vnode.dir, *hash, blob)
		if err != nil {
			return err
		}
	} else {
		l4g.Info("Making rpc call to add to relevant remote server [%s]", resp_vnode.host_addr)
		return b.storeRemoteBlob(blob, hash, resp_vnode.host_addr)
	}
	return nil
}

func (b *BlobStore) storeRemoteBlob(blob *[]byte, hash *string, server string) os.Error {
	conn, err := rpc.DialHTTP("tcp", server)
	if err != nil {
		return err
	}

	return conn.Call("BlobStore.Put", blob, hash)
}

func (b *BlobStore) storeBlob(location, name string, blob *[]byte) os.Error {
	dir_path, full_path := b.buildBlobPath(location, name)
	if filepath.HasPrefix(b.rootDir, full_path) {
		msg := fmt.Sprintf("Base path[%s] of vnode wasn't the root dir[%s]", full_path, b.rootDir)
		return os.NewError(msg)
	}

	l4g.Info("Storing blob under: %s", full_path)
	err := os.MkdirAll(dir_path, 0700)
	if err != nil {
		return err
	}

	_, err = os.Stat(full_path)
	if err == nil {
		// blob with same name exists.
		// means we are done.
		// TODO: check to verify size of the blobs are same
		//       and log that we tried to add existing blob.
		l4g.Info("Blob already exists at %s", full_path)
		return nil
	}
	return ioutil.WriteFile(full_path, *blob, 0600)

}

func (b *BlobStore) getBlob(location, name string) ([]byte, os.Error) {
	_, full_path := b.buildBlobPath(location, name)
	if filepath.HasPrefix(b.rootDir, full_path) {
		msg := fmt.Sprintf("Base path[%s] of vnode wasn't the root dir[%s]", full_path, b.rootDir)
		return nil, os.NewError(msg)
	}

	l4g.Info("Getting blob from: %s", full_path)

	_, err := os.Stat(full_path)
	if err != nil {
		msg := fmt.Sprintf("Blob not found under: %s", full_path)
		return nil, os.NewError(msg)
	}
	return ioutil.ReadFile(full_path)

}

func (b *BlobStore) getRemoteBlob(hash *string, blob *[]byte, server string) os.Error {
	conn, err := rpc.DialHTTP("tcp", server)
	if err != nil {
		return err
	}
	return conn.Call("BlobStore.Get", hash, blob)
}

func (b *BlobStore) buildBlobPath(location, name string) (dir, file string) {
	// ignores the 4th char (assumes it the separator between algo name and
	// hash
	return filepath.Clean(filepath.Join(b.rootDir, location, name[0:4], name[5:8], name[8:11])),
		filepath.Clean(filepath.Join(b.rootDir, location, name[0:4], name[5:8], name[8:11], name))

}
