package blobstore

// expects zookeeper to be running  on localhost:2181

import (
	"testing"
	"sort"
	"fmt"
)

func Test_AddRemoveVnode(t *testing.T) {
	ks := getTestKeySpace()
	vnode := &Vnode{offset: -1, hostname: "abcd"}

	ks.AddVnode(vnode)
	err := ks.RemoveVnode(vnode)
	if err != nil {
		t.Errorf("No error expected. got %q", err)
	}
}
func Test_GetVnodes(t *testing.T) {
	ks := getTestKeySpace()
	offsets := [...]int{10, 20, -10, -1, 0, 500}
	for i := range offsets {
		vnode := &Vnode{offset: offsets[i], hostname: fmt.Sprintf("abcd%d", offsets[i])}
		ks.AddVnode(vnode)
		defer ks.RemoveVnode(vnode)
	}

	vnodes, err := ks.GetVnodes()
	if err != nil {
		t.Errorf("No error expected. got %q", err)
	}

	if !sort.IsSorted(vnodes) {
		t.Error("GetVnodes did not return a sorted list.")
	}
	e := len(offsets)
	o := len(vnodes)
	if e != o {
		t.Errorf("GetVnodes did not return the correct number of vnodes. Expected %d, got %d", e, o)
	}
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
