package blobstore

import (
	. "launchpad.net/gocheck"
)

func (s *Unit) TestReplicas(c *C) {
	offsets := [...]int{-1e3, 100, 1e4, 1e6, 5e8, 6e8}
	vnodes := make(VnodeArray, 0)
	for i := range offsets {
		vnodes = append(vnodes, &Vnode{offset: offsets[i]})
	}
	r := &SimpleRep{N: 2}
	key := "sha1-1234" //hash is 531291731

	expected_replicas := [...]int{5e8, 6e8, -1e3}
	o, _ := r.Replicas(key, vnodes)

	c.Assert(len(expected_replicas), Equals, len(o))

	for i := range expected_replicas {
		c.Assert(o[i].GetOffset(), Equals, expected_replicas[i])
	}
}
func (s *Unit) TestReplicas_NumVnodesLessThatReplicationFactor(c *C) {
	offsets := [...]int{-50, -40, -30, -20}
	vnodes := make(VnodeArray, 0)
	for i := range offsets {
		vnodes = append(vnodes, &Vnode{offset: offsets[i]})
	}
	r := &SimpleRep{N: len(vnodes) + 1}
	hash := "sha1-1234"
	_, err := r.Replicas(hash, vnodes)

	c.Assert(err, Not(IsNil))
}
