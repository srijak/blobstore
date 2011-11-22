package blobstore

import "testing"

func TestGetHash_PrependAlgoToHash(t *testing.T) {
	i := "srijak"
	o := getHash([]byte(i))
	e := "sha1-c57deab7027fd806240c33324947c5b184e60adf"
	if o != e {
		t.Errorf("%q expected. got %q", e, o)
	}
}
