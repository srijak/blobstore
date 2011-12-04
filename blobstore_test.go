package blobstore

import (
	. "launchpad.net/gocheck"
	"os"
	"time"
	"fmt"
)

func (s *Unit) TestBlobGet_isLocal_FoundLocally(c *C) {
	bs := getTestBlobStore()

	blob := []byte("srijak") // hash: 929845509
	key := GetHash(&blob)

	// put data into local vnode using ILocalStore
	hostname, _ := os.Hostname()
	err := bs.ls.Put(&blob, key, &Vnode{offset: 9e8, hostname: hostname})
	c.Check(err, IsNil)

	// use blobstore to get the data.
	got := make([]byte, 0)
	err = bs.Get(&key, &got)
	c.Check(err, IsNil)
	c.Assert(string(got), Equals, string(blob))
}

func (s *Unit) TestBlobGet_isLocal_NotFoundLocally(c *C) {
	// if a blob should be found locally, but isn't then:
	//   get the blob from the remote server.
	//	 return the blob AND
	//   copy locally so next get will work locally.
	bs := getTestBlobStore()

	blob := []byte("srijak") // hash: 929845509
	key := GetHash(&blob)

	// put blob on remote replica node
	rc, err := bs.rsf.GetClient("notus1", bs.port)
	err = rc.Put(&blob, &key)
	c.Check(err, IsNil)

	// verify blob not on local
	hostname, _ := os.Hostname()
	_, err = bs.ls.Get(key, &Vnode{offset: 9e8, hostname: hostname})
	c.Check(err, Not(IsNil))

	// get. This will get from remote and copy into local.
	got := make([]byte, 0)
	err = bs.Get(&key, &got)
	c.Check(err, IsNil)
	c.Assert(string(got), Equals, string(blob))

	// wait a seconf to let the goroutine complete.
	time.Sleep(1e9)
	got_local, err := bs.ls.Get(key, &Vnode{offset: 9e8, hostname: hostname})
	c.Check(err, IsNil)
	c.Assert(string(got_local), Equals, string(blob))

}

func (s *Unit) TestBlobGet_isRemote_Forwarded(c *C) {
	bs := getTestBlobStore()

	blob := []byte("notsrijak") // hash: 2030091525
	var key string

	// put blob on remote replica node
	rc, err := bs.rsf.GetClient("notus2", bs.port)
	err = rc.Put(&blob, &key)
	c.Check(err, IsNil)

	// verify that we can get the blob when we hit blobstore
	// ie the call is forwarded tot he correct remote store.
	got := make([]byte, 0)
	bs.Get(&key, &got)
	c.Assert(string(got), Equals, string(blob))
}

func (s *Unit) TestBlobPut_isLocal_StoredLocally(c *C) {
	bs := getTestBlobStore()

	blob := []byte("srijak") // hash: 929845509
	var key string

	// put the data
	err := bs.Put(&blob, &key)
	c.Check(err, IsNil)

	// verify the data is in localstore
	hostname, _ := os.Hostname()
	got, err := bs.ls.Get(key, &Vnode{offset: 9e8, hostname: hostname})

	c.Check(err, IsNil)
	c.Assert(string(got), Equals, string(blob))
}

func (s *Unit) TestBlobPut_isRemote_StoredRemotely(c *C) {
	bs := getTestBlobStore()

	blob := []byte("notsrijak") // hash:  2030091525
	var key string

	// put the data
	err := bs.Put(&blob, &key)
	c.Check(err, IsNil)

	// verify the data is in relevant IRemotStore
	vnodes, err := bs.ks.GetVnodes()
	replicas, err := bs.rs.Replicas(key, vnodes)
	c.Check(err, IsNil)

	// wait a sec for go routines to put data on all replicas
	time.Sleep(1e9)
	for _, replica := range replicas {
		var got []byte
		rc, err := bs.rsf.GetClient(replica.GetHostname(), bs.port)
		err = rc.Get(&key, &got)
		c.Check(err, IsNil)
		c.Assert(string(got), Equals, string(blob))
	}
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
	rs := &SimpleRep{N: 2}
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
