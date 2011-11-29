package blobstore

import (
	"testing"
	"sort"
)

func TestSearchVnodes(t *testing.T) {
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
		if o != expected[i] {
			t.Errorf("Hash %d. Got offset %d. But expected to be put into offset %d\n", tests[i], o, expected[i])
		}
	}
}

func TestVnodeArray_Sort(t *testing.T) {
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
		if vnodes[i].GetOffset() < prev {
			t.Errorf("%d came before %d. Not sorted correctly.", vnodes[i].GetOffset(), prev)
		}
		prev = vnodes[i].GetOffset()
	}
}

func TestGetHash_PrependAlgoToHash(t *testing.T) {
	i := []byte("srijak")
	o := getHash(&i)
	e := "sha1-c57deab7027fd806240c33324947c5b184e60adf"
	if o != e {
		t.Errorf("%q expected. got %q", e, o)
	}
}
