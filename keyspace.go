package blobstore

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"launchpad.net/gozk"
	"hash/fnv"
	"strings"
	l4g "log4go.googlecode.com/hg"
)

const VALUE_SEPARATOR = "!"

type IKeySpace interface {
	Connect() (err os.Error)
	AddVnode(offset int, host string) (name string, err os.Error)
	RemoveVnode(offset int) (err os.Error)
	GetResponsibleVnode(str string) (vnode IVnode, err os.Error)
	GetVnodeOffsets() (offsets []int, err os.Error)
	GetVnode(offset int) (vnode IVnode, err os.Error)
}

type KeySpace struct {
	zkRoot    string
	zkServers string
	zkTimeout int64
	zk        *gozk.ZooKeeper
}

type IVnode interface {
	GetHostAddress() string
	GetDirectory() string
	IsLocal() bool
	String() string
}
type Vnode struct {
	offset    int
	host_addr string
	dir       string
}

func (vn *Vnode) IsLocal() bool {
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	return strings.Split(vn.host_addr, ":")[0] == host
}

func (vn *Vnode) GetDirectory() string {
	return vn.dir
}

func (vn *Vnode) GetHostAddress() string {
	return vn.host_addr
}

func (vn *Vnode) String() string {
	return fmt.Sprintf("%d@%s:%s", vn.offset, vn.host_addr, vn.dir)
}

func NewVnode(offset int, node_value string) *Vnode {
	v := new(Vnode)
	v.offset = offset

	if node_value != "" {
		v.host_addr = node_value
		v.dir = fmt.Sprintf("%d", offset)
	}
	return v
}

func EmptyVnode() *Vnode {
	return new(Vnode)
}

func NewKeySpace(rootNode string, servers string, timeout int64) *KeySpace {
	if rootNode == "" {
		rootNode = "/blobstore.keyspace"
	}
	if servers == "" {
		panic("KeySpace needs at least one server:port to connect to.")
	}
	k := new(KeySpace)
	k.zkRoot = rootNode
	k.zkServers = servers
	k.zkTimeout = timeout

	return k
}

func (k *KeySpace) Connect() (err os.Error) {
	zk, session, err := gozk.Init(k.zkServers, k.zkTimeout)
	if err != nil {
		return os.NewError("Couldn't connect: " + err.String())
	}

	event := <-session
	if event.State != gozk.STATE_CONNECTED {
		return os.NewError("Couldn't connect to zookeeper\n")
	}
	k.zk = zk

	stat, err := k.zk.Exists(k.zkRoot)
	if stat == nil {
		path, err := k.zk.Create(k.zkRoot, "root", 0, gozk.WorldACL(gozk.PERM_ALL))
		if err != nil {
			l4g.Info("Created root node: %s\n", path)
		}
	}

	return nil
}

func SetZooKeeperLogLevel(level int) {
	gozk.SetLogLevel(level)
}

func (k *KeySpace) GetVnodeOffsets() (offsets []int, err os.Error) {

	ret := make([]int, 0)
	children, _, err := k.zk.Children(k.zkRoot)

	if err != nil {
		return nil, err
	}

	for i := range children {
		n, err := strconv.Atoi(children[i])
		if err == nil {
			ret = append(ret, n)
		}
	}
	sort.Ints(ret)

	return ret, err
}

func (k *KeySpace) AddVnode(offset int, host string) (name string, err os.Error) {

	node := k.getVnodeString(offset)
	value := host

	stat, err := k.zk.Exists(node)
	if stat != nil {
		return node, os.NewError("Node already exists.")
	}
	if err != nil {
		return node, err
	}

	path, err := k.zk.Create(node, value, 0, gozk.WorldACL(gozk.PERM_ALL))
	if err != nil {
		return node, err
	}

	return path, nil
}

func (k *KeySpace) GetVnodeValue(offset int) (value string, err os.Error) {
	node := k.getVnodeString(offset)
	data, _, err := k.zk.Get(node)
	if err != nil {
		return "", err
	}

	return data, nil
}

func (k *KeySpace) GetResponsibleVnode(str string) (vnode IVnode, err os.Error) {
	offset, err := k.GetResponsibleOffset(str)
	if err != nil {
		return EmptyVnode(), err
	}
	return k.GetVnode(offset)
}

func (k *KeySpace) GetVnode(offset int) (vnode IVnode, err os.Error) {
	val, err := k.GetVnodeValue(offset)
	if err != nil {
		return nil, err
	}
	vn := NewVnode(offset, val)
	return vn, nil
}

func (k *KeySpace) GetResponsibleOffset(str string) (offset int, err os.Error) {
	hasher := fnv.New32a()
	hasher.Write([]byte(str))
	hash := int(hasher.Sum32())

	offsets, err := k.GetVnodeOffsets()
	if err != nil {
		return -1, err
	}
	if len(offsets) == 0 {
		return -1, os.NewError("No vnodes found")
	}
	return offsets[k.getResponsibleOffsetHelper(hash, offsets)], nil

}

func (k *KeySpace) getResponsibleOffsetHelper(hash int, offsets []int) (offsetIdx int) {
	// [myoffset, nextoffset)
	for i := range offsets {
		if hash == offsets[i] {
			return i
		}
	}
	num := sort.SearchInts(offsets, hash)
	//the last offset wraps around to the end of the first one
	if num == 0 {
		num = len(offsets)
	}
	return num - 1
}

func (k *KeySpace) RemoveVnode(offset int) (err os.Error) {

	node := k.getVnodeString(offset)

	stat, err := k.zk.Exists(node)
	if stat == nil {
		return os.NewError("Node doesn't exist. Cowardly refusing to delete.")
	}

	return k.zk.Delete(node, -1)
}

func (k *KeySpace) getVnodeString(offset int) string {

	return fmt.Sprintf("%s/%d", k.zkRoot, offset)
}
