package blobstore

import (
	"testing"
	"os"
	l4g "log4go.googlecode.com/hg"
)

func TestGetHash_Put(t *testing.T) {
	SetZooKeeperLogLevel(0)
	b := NewBlobStore()
	host, _ := os.Hostname()
	b.ks.AddVnode(-1, host, "-1")

	blob := []byte("srijak")

	hash, err := b.Put(blob)
	if err != nil {
		l4g.Error("Error Putting blob at hash: %s \nError:%s", hash, err.String())
	}

	l4g.Info("Successfully Put blob. Hash: " + hash)

	got, err := b.Get(hash)

	if err != nil {
		l4g.Error("Error Getting blob at hash: %s \nError:%s ", hash, err.String())
	}
	l4g.Info("Successfully Got blob: [%s]\n at hash: %s", string(got), hash)
}
