package blobstore

import (
	"testing"
	//	"fmt"
)

func TestReplicas(t *testing.T) {
	offsets := [...]int{-1e3, 100, 1e4, 1e6, 5e8, 6e8}
	vnodes := make(VnodeArray, 0)
	for i := range offsets {
		vnodes = append(vnodes, &Vnode{offset: offsets[i]})
	}
	r := &SimpleRep{N: 2}
	key := "sha1-1234" //hash is 531291731

	expected_replicas := [...]int{5e8, 6e8, -1e3}
	o, _ := r.Replicas(key, vnodes)

	if len(expected_replicas) != len(o) {
		t.Errorf("Got %d replicas. Expected: %d", len(o), len(expected_replicas))
	}
	for i := range expected_replicas {
		offset := o[i].GetOffset()
		if offset != expected_replicas[i] {
			t.Errorf("Got: %d as replication offset. Expected: %d", offset, expected_replicas[i])
		}
	}
}
func TestReplicas_NumVnodesLessThatReplicationFactor(t *testing.T) {
	offsets := [...]int{-50, -40, -30, -20}
	vnodes := make(VnodeArray, 0)
	for i := range offsets {
		vnodes = append(vnodes, &Vnode{offset: offsets[i]})
	}
	r := &SimpleRep{N: len(vnodes) + 1}
	hash := "sha1-1234"
	_, err := r.Replicas(hash, vnodes)
	if err == nil {
		t.Errorf("Got: No error. Expected: Error because num vnodes < replication factor")
	}
}
