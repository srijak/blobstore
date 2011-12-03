package blobstore

import (
	"fmt"
	"crypto/sha1"
	"sort"
)

func GetHash(blob *[]byte) string {
	sha1 := sha1.New()
	sha1.Write(*blob)
	return fmt.Sprintf("sha1-%x", sha1.Sum())
}

type VnodeArray []IVnode

func (va VnodeArray) Len() int {
	return len(va)
}
func (va VnodeArray) Swap(i, j int) {
	va[i], va[j] = va[j], va[i]
}
func (va VnodeArray) Less(i, j int) bool {
	return va[i].GetOffset() < va[j].GetOffset()
}

func SearchVnode(vnodes VnodeArray, hash int) (index int) {
	if !sort.IsSorted(vnodes) {
		sort.Sort(&vnodes)
	}

	num := sort.Search(len(vnodes), func(i int) bool {
		// [myoffset, nextoffset)
		return vnodes[i].GetOffset() > hash
	})
	//the last offset wraps around to the end of the first one
	if num == 0 {
		num = len(vnodes)
	}
	return num - 1
}
