package blobstore

import (
	"os"
	"fmt"
	"hash/fnv"
)

type IReplicationStrategy interface {
	Replicas(key string, allVnodes VnodeArray) (VnodeArray, os.Error)
}

// Replicated to N next vnodes.
// Expects sequential vnodes to be on different hosts.
type SimpleRep struct {
	N int // replication factor
}

func (r *SimpleRep) Replicas(key string, allVnodes VnodeArray) (VnodeArray, os.Error) {
	replicas := VnodeArray{}

	if len(allVnodes) < r.N {
		msg := fmt.Sprintf("Not enough vnodes for replicationFactor %d", r.N)
		return replicas, os.NewError(msg)
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := int(hasher.Sum32())

	start_idx := SearchVnode(allVnodes, hash)

	for i := start_idx; len(replicas) <= r.N; i++ {
		idx := i % len(allVnodes)
		replicas = append(replicas, allVnodes[idx])
	}

	return replicas, nil
}
