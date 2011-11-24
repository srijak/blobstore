package blobstore

import (
	"fmt"
	"crypto/sha1"
)

func getHash(blob *[]byte) string {
	sha1 := sha1.New()
	sha1.Write(*blob)
	return fmt.Sprintf("sha1-%x", sha1.Sum())
}
