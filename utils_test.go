package blobstore

import (
	. "launchpad.net/gocheck"
	"sort"
)

func (s *Unit) TestSearchVnodes(c *C) {
	offsets := [...]int{-20, -10, 0, 30, 50, 100}
	tests := [...]int{-21, -19, 5, 35, 1001, 7, -20, -10, 0, 30, 50, 100}
	expected := [...]int{100, -20, 0, 30, 100, 0, -20, -10, 0, 30, 50, 100}

	vnodes := make(VnodeArray, 0)
	for i := range offsets {
		vnodes = append(vnodes, &Vnode{offset: offsets[i]})
	}
	for i := range tests {
		idx := SearchVnode(vnodes, tests[i])
		o := vnodes[idx].GetOffset()
		c.Assert(o, Equals, expected[i])
	}
}

func (s *Unit) TestVnodeArray_Sort(c *C) {
	vnodes := VnodeArray{
		&Vnode{offset: 50},
		&Vnode{offset: -20},
		&Vnode{offset: -2},
		&Vnode{offset: 500},
		&Vnode{offset: -20},
		&Vnode{offset: 20},
		&Vnode{offset: 0},
	}
	sort.Sort(&vnodes)
	prev := -5000 // item smaller than all items in list
	for i := range vnodes {
		c.Check(vnodes[i].GetOffset() >= prev, Equals, true)
		prev = vnodes[i].GetOffset()
	}
}

func (s *Unit) TestGetHash_PrependAlgoToHash(c *C) {
	i := []byte("srijak")
	o := getHash(&i)
	e := "sha1-c57deab7027fd806240c33324947c5b184e60adf"
	c.Assert(o, Equals, e)
}
