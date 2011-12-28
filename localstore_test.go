package blobstore

import (
	. "launchpad.net/gocheck"
	"io/ioutil"
	"bytes"
)

func (s *Unit) TestPutGet(c *C) {

	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	dat := "srijak"
	dat_hash := "sha1-c57deab7027fd806240c33324947c5b184e60adf"

	r := bytes.NewBufferString(dat)
	w := &bytes.Buffer{}
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)
	ds.Put(dat_hash, vn, r)

	size, err := ds.Get(dat_hash, vn, w)
	c.Check(err, IsNil)
	c.Assert(w.String(), Equals, dat)
	c.Assert(size, Equals, int64(len(dat)))
}

func (s *Unit) TestPut_NotUnderRootDir_ReturnsError(c *C) {

	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	r := bytes.NewBufferString("")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	_, err = ds.Put("../../../tmp/sha1.blah", vn, r)
	c.Assert(err, Not(IsNil))
}

func (s *Unit) TestGet_NonExistantBlob_ReturnsError(c *C) {

	tmp_dir, err := ioutil.TempDir("", "localstore_test")
	c.Check(err, IsNil)

	w := bytes.NewBufferString("")
	vn, _ := NewVnodeFromString("-1@abcd")

	ds := NewDiskStore(tmp_dir)

	_, err = ds.Get("sha1-NONEXISTENT", vn, w)
	c.Assert(err, Not(IsNil))
}
