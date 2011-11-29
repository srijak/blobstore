package blobstore

import (
	"testing"
	"io/ioutil"
)

func TestPutGet(t *testing.T) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	if err != nil {
		t.Error(err.String())
	}
	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)
	ds.Put(blob, getHash(blob), vn)

	got, err := ds.Get(getHash(blob), vn)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.String())
	}
	if string(got) != string(*blob) {
		t.Errorf("Got: %s. Expected: %s", string(got), string(*blob))
	}
}
func TestPut_NotUnderRootDir(t *testing.T) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	if err != nil {
		t.Error(err.String())
	}
	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	err = ds.Put(blob, "../../../tmp/sha1.blah", vn)

	if err == nil {
		t.Error("Expected error since we tried to put data above rootDir. But, got no error.")
	}
}
func TestGet_NonExistantBlob(t *testing.T) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	if err != nil {
		t.Error(err.String())
	}
	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	got, err := ds.Get(getHash(blob), vn)
	if err == nil {
		t.Errorf("Expected error since blob doesn't exist. But, got no error. Got blob: %s", string(got))
	}
}
