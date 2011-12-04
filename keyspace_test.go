package blobstore

// expects zookeeper to be running  on localhost:2181

import (
	. "launchpad.net/gocheck"
	"sort"
	"fmt"
)

func (s *Integration) Test_AddRemoveVnode(c *C) {
	ks := getTestKeySpace()
	vnode := &Vnode{offset: -1, hostname: "abcd"}

	ks.AddVnode(vnode)
	err := ks.RemoveVnode(vnode)
	c.Assert(err, IsNil)
}

func (s *Integration) Test_GetVnodes(c *C) {
	ks := getTestKeySpace()
	offsets := [...]int{10, 20, -10, -1, 0, 500}
	for i, o := range offsets {
		vnode := &Vnode{offset: offsets[i], hostname: fmt.Sprintf("abcd%d", o)}
		ks.AddVnode(vnode)
		defer ks.RemoveVnode(vnode)
	}

	vnodes, err := ks.GetVnodes()
	c.Check(err, IsNil)

	c.Assert(sort.IsSorted(vnodes), Equals, true)
	c.Assert(len(offsets), Equals, len(vnodes))
}

func getTestKeySpace() IKeySpace {
	k := NewKeySpace("/keyspacetest", "localhost:2181", 5e3)
	SetZooKeeperLogLevel(0)

	err := k.Connect()
	if err != nil {
		panic("Error:" + err.String())
	}

	return k
}
