package blobstore

import (
	"testing"
	"os"
	l4g "log4go.googlecode.com/hg"
)

func TestBlobStore_PutGet(t *testing.T) {
	SetZooKeeperLogLevel(0)
	host, _ := os.Hostname()

	ks := NewKeySpace("/blobstore.keyspace", "localhost:2181", 5e6)
	ks.Connect()
	ks.AddVnode(-1, host+":8080", "-1")

	b := NewBlobStore("/tmp/vnodes", ks)

	blob := &[]byte("srijak")
	hash := new(string)
	err := b.Put(blob, hash)
	if err != nil {
		l4g.Error("Error Putting blob at hash: %s \nError:%s", *hash, err.String())
		return
	} else {
		l4g.Info("Successfully Put blob. Hash: " + *hash)
	}
	got_blob := new([]byte)
	err = b.Get(hash, got_blob)

	if err != nil {
		l4g.Error("Error Getting blob at hash: %s \nError:%s ", *hash, err.String())
	} else {
		l4g.Info("Successfully Got blob: [%s]\n at hash: %s", string(*got_blob), *hash)
		return
	}
}
