package blobstore

import (
	"os"
	l4g "log4go.googlecode.com/hg"
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
	var local_vnode_idx int
	for i, replica := range replicas {
		// look for a local one first.

		if isLocalVnode(replica) {
			// this code assumes that there is only one matching local vnode.
			// i.e that replicas aren't on the same server.
			l4g.Debug("Key should be on local vnode: %s", replica)
			*blob, err = b.ls.Get(*key, replica)
			if err != nil {
				// err is assumed to be coz data was missing.
				// so, copy it locally once we get it.
				l4g.Debug("Key not found on local vnode. Will read repair vode %s for key: %s", replica, *key)
				copyLocally = true
				local_vnode_idx = i
			} else {
				// if no errors getting data locally,
				// we are done
				//TODO: verify data: 
				//if hash doesn't match, get it from another replica.
				l4g.Debug("Found data for key %s locally.", *key)
				return nil
			}
		}
	}
	// the fact that we are here means data wasn't found locally.
	// if copyLocally is true, it means that we should store
	// the data locally coz it should have been here.
	var local_vnode IVnode
	if copyLocally {
		//remove local vnode from list so it isn't tried again.
		local_vnode = replicas[local_vnode_idx]
		replicas = append(replicas[:local_vnode_idx], replicas[local_vnode_idx+1:]...)
	}
	l4g.Debug("Data for key %s not found locally. Trying other replicas.", *key)
	for _, replica := range replicas {
		err = b.getRemoteBlob(key, blob, replica)
		if err == nil {
			l4g.Debug("Got data for key %s from remote: %s", *key, replica.String())
			if copyLocally {

				l4g.Debug("ReadRepair: Copying key %s to local vnode: %s", *key, local_vnode.String())

				go b.ls.Put(blob, *key, local_vnode)
			}
			return nil
		} else {
			l4g.Info("Couldnt get key %s from remote replica %s. Error: %s", *key, replica, err.String())
		}
	}

	return os.NewError("Could not get from any of the replicas")
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

func (b *BlobStore) Put(blob *[]byte, key *string) os.Error {
	// TODO: get and put are very similiar. Refactor to reuse logic
	vnodes, err := b.ks.GetVnodes()
	if err != nil {
		return err
	}
	*key = GetHash(blob)
	replicas, err := b.rs.Replicas(*key, vnodes)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		// look for a local one first.
		if isLocalVnode(replica) {
			l4g.Debug("Data should be on local vnode: %s", replica)
			err := b.ls.Put(blob, *key, replica)
			if err != nil {
				// local put failed for some reason.
				// TODO: how to handle?
				return err
			} else {
				// if no errors putting data locally,
				return nil
			}
		}
	}
	// the fact that we are here means it's not a local put or that the local
	// put failed.
	// Try to store on any replica. Note that we retry local here.
	// also, we require one synchronous successfull write.
	// the number of writes that need to succeed will be customizable.
	l4g.Debug("Data should be on remote vnode.")
	one_passed := false
	for _, replica := range replicas {

		l4g.Debug("Trying to store to %s.", replica.String())
		if !one_passed {
			err = b.putRemoteBlob(blob, key, replica)
			if err == nil {
				l4g.Debug("Data stored on vnode %s.", replica.String())
				one_passed = true
			}
		} else {
			go b.putRemoteBlob(blob, key, replica)
		}
	}

	if one_passed {
		return nil
	}
	return os.NewError("Could not put to any responsible nodes. TODO: hinted handoff")
}
