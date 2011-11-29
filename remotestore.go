package blobstore

import (
	"os"
	"fmt"
	"rpc"
)

type IRemoteStore interface {
	Put(blob *[]byte, key *string) os.Error
	Get(key *string, blob *[]byte) os.Error
}

type IRemoteStoreFactory interface {
	GetClient(hostname string, port int) (IRemoteStore, os.Error)
}

type RemoteStoreFactory struct{}

func (r *RemoteStoreFactory) GetClient(hostname string, port int) (IRemoteStore, os.Error) {
	//cache etcs later, if reqd.
	return NewRemoteStore(hostname, port)
}

type RemoteStore struct {
	client *rpc.Client
	host   string
	port   int
}

func NewRemoteStore(host string, port int) (*RemoteStore, os.Error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &RemoteStore{host: host, port: port, client: c}, nil
}
func (rs *RemoteStore) Put(blob *[]byte, key *string) os.Error {
	return rs.client.Call("BlobStore.Put", blob, key)
}

func (rs *RemoteStore) Get(key *string, blob *[]byte) os.Error {
	return rs.client.Call("BloblStore.Get", key, blob)
}
