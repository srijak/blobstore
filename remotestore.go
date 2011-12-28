package blobstore

import (
	"os"
	"io"
)

type IRemoteStoreFactory interface {
	GetClient(hostname string, port int) (IBlobStore, os.Error)
}

type RemoteStoreFactory struct{}

func (r *RemoteStoreFactory) GetClient(hostname string, port int) (IBlobStore, os.Error) {
	//cache etcs later, if reqd.
	return NewRemoteStore(hostname, port)
}

type RemoteStore struct {
	host string
	port int
}

func NewRemoteStore(host string, port int) (*RemoteStore, os.Error) {
	//addr := fmt.Sprintf("%s:%d", host, port)
	// TODO: connect to store.
	return &RemoteStore{host: host, port: port}, nil
}
func (rs *RemoteStore) Put(key *string, r io.Reader) os.Error {
	return os.NewError("Not implemented")
}

func (rs *RemoteStore) Get(key *string, w io.Writer) os.Error {
	return os.NewError("Not implemented")
}
