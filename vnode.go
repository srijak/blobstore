package blobstore

import (
	"fmt"
	"os"
	"strings"
	"strconv"
)

type IVnode interface {
	GetHostname() string
	GetOffset() int
	String() string
}

type Vnode struct {
	offset   int
	hostname string
}

func (vn *Vnode) GetHostname() string {
	return vn.hostname
}

func (vn *Vnode) GetOffset() int {
	return vn.offset
}

func (vn *Vnode) String() string {
	return fmt.Sprintf("%d@%s", vn.offset, vn.hostname)
}

func NewVnode(offset int, host string) IVnode {
	return &Vnode{offset: offset, hostname: host}
}
func NewVnodeFromString(str string) (IVnode, os.Error) {
	v := new(Vnode)
	vals := strings.Split(str, "@")
	if len(vals) != 2 {
		return v, os.NewError("String expected in format int@hostname")
	}
	v.hostname = vals[1]
	offset, err := strconv.Atoi(vals[0])
	v.offset = offset
	if err != nil {
		return v, err
	}

	return v, nil
}

func EmptyVnode() IVnode {
	return new(Vnode)
}
