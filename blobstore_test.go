package blobstore

import (
	. "launchpad.net/gocheck"
	"os"
	"fmt"
)

func (s *Unit) TestBlobGet_isLocal_FoundLocally(c *C) {
	bs := getTestBlobStore()

	blob := []byte("srijak") // hash: 929845509
	var key string

	err := bs.Put(&blob, &key)
	c.Check(err, IsNil)
	got := make([]byte, 0)
	err = bs.Get(&key, &got)
	c.Check(err, IsNil)
	c.Assert(string(got), Equals, string(blob))
}

func (s *Unit) TestBlobGet_isRemote_Forwarded(c *C) {
	bs := getTestBlobStore()

	blob := []byte("notsrijak") // hash: 2030091525
	var key string

	bs.Put(&blob, &key)

	got := make([]byte, 0)
	bs.Get(&key, &got)

	c.Assert(string(got), Equals, string(blob))
}

func getTestBlobStore() *BlobStore {
	hostname, _ := os.Hostname()

	vnodes := VnodeArray{
		&Vnode{offset: 10, hostname: "notus0"},
		&Vnode{offset: 9e8, hostname: hostname}, // contains srijak
		&Vnode{offset: 1e9, hostname: "notus1"},
		&Vnode{offset: 2e9, hostname: "notus2"}, // contains notsrijak
	}

	ks := &TestKeySpace{vnodes: vnodes}
	rs := &SimpleRep{N: 1}
	ls := &TestLocalStore{make(map[string]string)}
	rsf := &TestRemoteStoreFactory{make(map[string]IRemoteStore)}

	return NewBlobStore(ks, rs, ls, rsf, 8089)
}

type TestRemoteStore struct {
	m map[string]string
}

func (rs *TestRemoteStore) Put(blob *[]byte, key *string) os.Error {
	rs.m[*key] = string(*blob)
	return nil
}

func (rs *TestRemoteStore) Get(key *string, blob *[]byte) os.Error {
	if val, ok := rs.m[*key]; ok {
		*blob = []byte(val)
		return nil
	}
	return os.NewError("Key not present.")
}

type TestRemoteStoreFactory struct {
	m map[string]IRemoteStore
}

func (r *TestRemoteStoreFactory) GetClient(hostname string,
port int) (IRemoteStore, os.Error) {
	name := fmt.Sprintf("%s:%d", hostname, port)
	if val, ok := r.m[name]; ok {
		return val, nil
	}
	trs := &TestRemoteStore{make(map[string]string)}

	r.m[name] = trs
	return trs, nil
}

type TestLocalStore struct {
	m map[string]string
}

func (t *TestLocalStore) Get(name string, vn IVnode) ([]byte, os.Error) {
	if val, ok := t.m[name]; ok {
		return []byte(val), nil
	}
	return make([]byte, 0), os.NewError("Key not present.")

}
func (t *TestLocalStore) Put(blob *[]byte, name string, vn IVnode) os.Error {
	t.m[name] = string(*blob)
	return nil
}

type TestKeySpace struct {
	vnodes VnodeArray
}

func (t *TestKeySpace) AddVnode(vn IVnode) os.Error {
	t.vnodes = append(t.vnodes, vn)
	return nil
}

func (t *TestKeySpace) RemoveVnode(vn IVnode) os.Error {
	return os.NewError("Not implemented for tests")
}

func (t *TestKeySpace) GetVnodes() (VnodeArray, os.Error) {
	return t.vnodes, nil
}
func (t *TestKeySpace) Connect() os.Error {
	return nil
}
