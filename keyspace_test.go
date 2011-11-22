package blobstore

// expects zookeeper running on localhost:2181

import (
	"testing"
	"fmt"
)

func TestKeySpace_AddVnode(t *testing.T) {
	// delete node, if it already exists
	k := getKeySpace()
	k.RemoveVnode(123)

	o, err := k.AddVnode(123, "localhost", "/tmp/a")
	if err != nil {
		t.Error(err.String())
	}

	e := k.zkRoot + "/123"
	if o != e {
		t.Errorf("%q expected. got %q", e, o)
	}
}

func TestKeySpace_GetVnodes(t *testing.T) {

	k := getKeySpace()
	ranges := [...]int{123, 345, 456, 678, 91011}
	for i := range ranges {
		// delete node, if it already exists
		k.RemoveVnode(ranges[i])
	}

	for i := range ranges {
		k.AddVnode(ranges[i], "localhost", "/tmp/"+string(ranges[i]))
	}

	vnodes, _ := k.GetVnodeOffsets()
	// ugh. why doesn't go have a set ds?
	// for now testing by count will ahve to suffice
	e := len(ranges)
	o := len(vnodes)

	//cleanup
	for i := range ranges {
		k.RemoveVnode(ranges[i])
	}

	if o != e {
		t.Errorf("%d vnodes expected. But got %d", e, o)
		for i := range vnodes {
			t.Error(vnodes[i])
		}
	}

}

func TestKeySpace_GetResponsibleOffsetHelper(t *testing.T) {
	k := getKeySpace()
	offsets := [...]int{-20, -10, 0, 30, 50, 100}
	tests := [...]int{-21, -19, 5, 35, 1001, 7, -20, -10, 0, 30, 50, 100}
	expected := [...]int{100, -20, 0, 30, 100, 0, -20, -10, 0, 30, 50, 100}

	for i := range tests {
		o := k.getResponsibleOffsetHelper(tests[i], offsets[:])
		if offsets[o] != expected[i] {
			t.Errorf("%d. got: %d. expected: %d\n", tests[i], offsets[o], expected[i])
		}
	}
}

func TestKeySpace_GetVnodeValue(t *testing.T) {
	k := getKeySpace()

	k.RemoveVnode(123)

	dir := "/tmp/ac"
	host := "localhost"
	exp := fmt.Sprintf("%s:%s", host, dir)

	_, err := k.AddVnode(123, host, dir)
	if err != nil {
		t.Error(err.String())
	}

	out, err := k.GetVnodeValue(123)
	if err != nil {
		t.Error(err.String())
	}
	if out != exp {
		t.Errorf("%q expected as vnode value. Got %q", exp, out)
	}
}

func getKeySpace() *KeySpace {
	k := NewKeySpace("/keyspacetest", "localhost:2181", 5e6)
	SetZooKeeperLogLevel(0)

	err := k.Connect()
	if err != nil {
		panic("Error:" + err.String())
	}

	return k
}
