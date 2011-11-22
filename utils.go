package blobstore

import (
	"fmt"
	"crypto/sha1"
)

// basically copied IntSlice form sort/sort.go
// is golang lame enough to require me to do this?
// ugh. Either generate these for all basic types
// OR, support generics.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func getHash(blob []byte) string {
	sha1 := sha1.New()
	sha1.Write(blob)
	return fmt.Sprintf("sha1-%x", sha1.Sum())
}
