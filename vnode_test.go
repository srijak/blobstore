package blobstore

import "testing"

func TestNewVnodeFromString(t *testing.T) {
	i := "-2342@abcd"
	o, err := NewVnodeFromString(i)
	if err != nil {
		t.Errorf("Unexpected error: %q", err)
	}
	expectedOffset := -2342
	expectedHostname := "abcd"

	if o.GetOffset() != expectedOffset {
		t.Errorf("%q expected. got %q", o.GetOffset(), expectedOffset)
	}
	if o.GetHostname() != expectedHostname {
		t.Errorf("%q expected. got %q", o.GetHostname(), expectedHostname)
	}
}

func TestNewVnodeFromString_NonIntOffset(t *testing.T) {
	i := "qwe@abcd"
	_, err := NewVnodeFromString(i)

	if err == nil {
		t.Error("Should have returned error.")
	}
}
func TestNewVnodeFromString_MissingOffset(t *testing.T) {
	i := "@abcd"
	_, err := NewVnodeFromString(i)

	if err == nil {
		t.Error("Should have returned error.")
	}
}

func TestNewVnodeFromString_NoAtSign(t *testing.T) {
	i := "qweabcd"
	_, err := NewVnodeFromString(i)

	if err == nil {
		t.Error("Should have returned error.")
	}
}

func TestNewVnodeFromString_to_String(t *testing.T) {
	i := "-2342@abcd"
	o, err := NewVnodeFromString(i)
	if err != nil {
		t.Errorf("Unexpected error: %q", err)
	}
	e := i
	if o.String() != e {
		t.Errorf("%q expected. got %q", e, o)
	}
}
