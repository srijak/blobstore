package blobstore

import (
	. "launchpad.net/gocheck"
)

func (s *Unit) TestNewVnodeFromString(c *C) {
	i := "-2342@abcd"
	o, err := NewVnodeFromString(i)
	c.Check(err, IsNil)

	expectedOffset := -2342
	expectedHostname := "abcd"

	c.Assert(o.GetOffset(), Equals, expectedOffset)
	c.Assert(o.GetHostname(), Equals, expectedHostname)
}

func (s *Unit) TestNewVnodeFromString_NonIntOffset(c *C) {
	i := "qwe@abcd"
	_, err := NewVnodeFromString(i)

	c.Assert(err, Not(IsNil))
}

func (s *Unit) TestNewVnodeFromString_MissingOffset(c *C) {
	i := "@abcd"
	_, err := NewVnodeFromString(i)

	c.Assert(err, Not(IsNil))
}

func (s *Unit) TestNewVnodeFromString_NoAtSign(c *C) {
	i := "qweabcd"
	_, err := NewVnodeFromString(i)

	c.Assert(err, Not(IsNil))
}

func (s *Unit) TestNewVnodeFromString_to_String(c *C) {
	i := "-2342@abcd"
	o, err := NewVnodeFromString(i)

	c.Check(err, IsNil)
	c.Assert(o.String(), Equals, i)
}
