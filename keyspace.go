package blobstore

import (
	"fmt"
	"os"
	"sort"
	"launchpad.net/gozk"
	l4g "log4go.googlecode.com/hg"
)

type IKeySpace interface {
	AddVnode(vnode IVnode) os.Error
	RemoveVnode(vnode IVnode) os.Error
	GetVnodes() (VnodeArray, os.Error)
	Connect() os.Error
}

type KeySpace struct {
	zkRoot    string
	zkServers string
	zkTimeout int64
	zk        *gozk.ZooKeeper
}

func (k *KeySpace) GetVnodes() (VnodeArray, os.Error) {
	ret := make(VnodeArray, 0)

	children, _, err := k.zk.Children(k.zkRoot)
	if err != nil {
		return nil, err
	}

	for _, vstr := range children {
		vn, err := NewVnodeFromString(vstr)
		if err == nil {
			ret = append(ret, vn)
		} else {
			return nil, err
		}

	}

	sort.Sort(ret)

	return ret, err
}

func (k *KeySpace) RemoveVnode(vnode IVnode) os.Error {
	path := k.getVnodePath(vnode)

	stat, err := k.zk.Exists(path)
	if err != nil {
		return err
	}
	if stat == nil {
		// should we just ignore this case? ie, if you try to delete
		// a vnode that doesn't exists, ignore?
		return os.NewError(fmt.Sprintf("Node doesn't exist at %s.", path))
	}

	return k.zk.Delete(path, -1)
}

func (k *KeySpace) AddVnode(vnode IVnode) os.Error {
	path := k.getVnodePath(vnode)

	stat, err := k.zk.Exists(path)
	if stat != nil {
		return os.NewError(fmt.Sprintf("Node already exists at %s", path))
	}
	if err != nil {
		return err
	}

	_, err = k.zk.Create(path, "0", 0, gozk.WorldACL(gozk.PERM_ALL))
	return err
}

func NewKeySpace(rootNode string, servers string, timeoutInMillis int64) *KeySpace {
	if rootNode == "" {
		rootNode = "/blobstore.keyspace"
	}
	if servers == "" {
		panic("KeySpace needs at least one server:port to connect to.")
	}

	return &KeySpace{zkRoot: rootNode, zkServers: servers, zkTimeout: timeoutInMillis * 1000}
}

func (k *KeySpace) Connect() os.Error {
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

func (k *KeySpace) getVnodePath(vnode IVnode) string {
	return fmt.Sprintf("%s/%s", k.zkRoot, vnode.String())
}
