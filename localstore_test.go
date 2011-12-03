package blobstore

import (
	. "launchpad.net/gocheck"
	"io/ioutil"
)

func (s *Unit) TestPutGet(c *C) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)
	ds.Put(blob, GetHash(blob), vn)

	got, err := ds.Get(GetHash(blob), vn)
	c.Check(err, IsNil)
	c.Assert(string(got), Equals, string(*blob))
}
func (s *Unit) TestPut_NotUnderRootDir_ReturnsError(c *C) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	err = ds.Put(blob, "../../../tmp/sha1.blah", vn)

	c.Assert(err, Not(IsNil))
}
func (s *Unit) TestGet_NonExistantBlob_ReturnsError(c *C) {
	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	blob := &[]byte("srijak")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	_, err = ds.Get(GetHash(blob), vn)
	c.Assert(err, Not(IsNil))
}
