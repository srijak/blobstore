package blobstore

import (
	"os"
	l4g "log4go.googlecode.com/hg"
	"rand"
	//	"fmt"
)

type IBlobStore interface {
	Put(blob *[]byte, key *string) os.Error
	Get(key *string, blob *[]byte) os.Error
}

type BlobStore struct {
	ks   IKeySpace
	rs   IReplicationStrategy
	ls   ILocalStore
	rsf  IRemoteStoreFactory
	port int
}

func NewBlobStore(ks IKeySpace,
rs IReplicationStrategy,
ls ILocalStore,
rsf IRemoteStoreFactory,
port int) *BlobStore {

	b := &BlobStore{ks: ks, rs: rs, ls: ls, rsf: rsf, port: port}
	b.ks.Connect()
	return b
}

func (b *BlobStore) Get(key *string, blob *[]byte) os.Error {
	// get replica list
	// TODO: remove servers that aren't currently up.
	// If we are in the replica list:
	//    return data
	//      if data not available locally, get data from other replicas
	//        (readrepair: store locally so next time we can field the request)
	// If not on us:
	//    hit one of the replicas for data, it will find and return data.
	vnodes, err := b.ks.GetVnodes()
	if err != nil {
		return err
	}
	replicas, err := b.rs.Replicas(*key, vnodes)
	if err != nil {
		return err
	}
	copyLocally := false
	for i := range replicas {
		// look for a local one first.
		if isLocalVnode(replicas[i]) {
			*blob, err = b.ls.Get(*key, replicas[i])
			if err != nil {
				// err is assumed to be coz data was missing.
				// so, copy it locally once we get it.
				copyLocally = true
			} else {
				// if no errors getting data locally,
				// we are done
				//TODO: verify data: 
				//if hash doesn't match, get it from another replica.
				return nil
			}
		}
	}
	// the fact that we are here means data wasn't found locally.
	// if copyLocally is true, it means that we should store
	// the data locally coz it should have been here.

	idx := rand.Intn(len(replicas))
	err = b.getRemoteBlob(key, blob, replicas[idx])
	if err != nil {
		//TODO: could try others.
		return err
	}
	if copyLocally {
		go b.ls.Put(blob, *key, replicas[idx])
	}
	return nil
}

func (b *BlobStore) getRemoteBlob(key *string, blob *[]byte, vn IVnode) os.Error {

	r, err := b.rsf.GetClient(vn.GetHostname(), b.port)
	if err != nil {
		return err
	}

	return r.Get(key, blob)
}

func (b *BlobStore) putRemoteBlob(blob *[]byte, key *string, vn IVnode) os.Error {

	r, err := b.rsf.GetClient(vn.GetHostname(), b.port)
	if err != nil {
		return err
	}

	return r.Put(blob, key)
}

func isLocalVnode(vn IVnode) bool {
	h, err := os.Hostname()
	if err != nil {
		l4g.Warn("Couldn't get os.Hostname: %s", err.String())
		return false
	}
	return vn.GetHostname() == h
}

func (b *BlobStore) Put(blob *[]byte, key *string) (err os.Error) {
	// TODO: get and put are very similiar. Refactor to reuse logic
	vnodes, err := b.ks.GetVnodes()
	if err != nil {
		return err
	}
	*key = getHash(blob)
	replicas, err := b.rs.Replicas(*key, vnodes)
	if err != nil {
		return err
	}
	for i := range replicas {
		// look for a local one first.
		if isLocalVnode(replicas[i]) {
			err := b.ls.Put(blob, *key, replicas[i])
			if err != nil {
				// local put failed for some reason.
				// TODO: how to handle?
				return err
			} else {
				// if no errors getting data locally,
				// we are done
				//TODO: verify data: 
				//if hash doesn't match, get it from another replica.

				return nil
			}
		}
	}
	// the fact that we are here means it's not a local put.
	// choose a random replica
	idx := rand.Intn(len(replicas))
	err = b.putRemoteBlob(blob, key, replicas[idx])
	if err != nil {
		//TODO: could try others.
		return err
	}

	return err
}
