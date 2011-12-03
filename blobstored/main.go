package main

import (
	"http"
	"fmt"
	"os"
	l4g "log4go.googlecode.com/hg"
	"rpc"
	. "github.com/srijak/blobstore"
	"flag"
	"github.com/srijak/configo"
	"strconv"
)

// bsd -c configfile daemonize
// bsd -c configfile add-vnode -121
// bsd -c configfile rm-vnode -121
// bsd -c configfile lm-vnodes
var confFile *string = flag.String("c", "blobstored.conf", "configuration file")

type ConfigOpts struct {
	ZkHosts           string
	ZkRootNode        string
	RootDir           string
	Port              int
	ReplicationFactor int
}

func loadConfigs() *ConfigOpts {
	flag.Parse()
	co := ConfigOpts{}
	configo.NewConfigo(*confFile).Hydrate(&co)
	return &co
}

func usage() {
	fmt.Printf("Usage: bsd [-c config file] <command>\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf(" daemonize\tstart a blobstore server.\n")
	fmt.Printf(" add-vnode <int offset>\tadd a new vnode.Needs to be run from server on which to add vnode.\n")
	fmt.Printf(" rm-vnode <int offset>\tremove the given vnode.Needs to be run from server from which to remove vnode.\n")
	fmt.Printf(" ls-vnodes\tlist current vnodes.\n")
	fmt.Printf("\n")
}

func runCommand(configs *ConfigOpts, command string, args []string) {
	var err os.Error
	switch {
	case command == "daemonize":
		err = daemonize(configs)
	case command == "add-vnode":
		err = addVnode(configs, args)
	case command == "rm-vnode":
		err = rmVnode(configs, args)
	case command == "ls-vnodes":
		err = lsVnodes(configs)
	default:
		usage()
	}
	if err != nil {
		l4g.Error(err)
	}
}

func addVnode(configs *ConfigOpts, args []string) os.Error {
	offset, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}

	k := getKeySpace(configs)

	host, err := os.Hostname()
	if err != nil {
		return err
	}
	return k.AddVnode(NewVnode(offset, host)) //currently only allow to add to local host
}

func rmVnode(configs *ConfigOpts, args []string) os.Error {
	offset, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}

	k := getKeySpace(configs)

	host, err := os.Hostname()
	if err != nil {
		return err
	}

	return k.RemoveVnode(NewVnode(offset, host))
}

func lsVnodes(configs *ConfigOpts) os.Error {
	k := getKeySpace(configs)
	vnodes, err := k.GetVnodes()
	if err != nil {
		return err
	}

	fmt.Printf("There are %d vnodes.\n", len(vnodes))
	for i := range vnodes {
		fmt.Printf("%s\n", vnodes[i].String())
	}
	return nil
}

func getKeySpace(configs *ConfigOpts) IKeySpace {
	k := NewKeySpace(configs.ZkRootNode, configs.ZkHosts, 5e6)
	SetZooKeeperLogLevel(0)

	err := k.Connect()
	if err != nil {
		panic("Error:" + err.String())
	}

	return k
}

func daemonize(configs *ConfigOpts) os.Error {
	ks := NewKeySpace(configs.ZkRootNode, configs.ZkHosts, 5e6)
	ks.Connect()
	rs := &SimpleRep{N: configs.ReplicationFactor}
	ls := NewDiskStore(configs.RootDir)
	rsf := &RemoteStoreFactory{}
	b := NewBlobStore(ks, rs, ls, rsf, configs.Port)
	rpc.Register(b)
	rpc.HandleHTTP()
	addr := fmt.Sprintf(":%d", configs.Port)
	l4g.Info("Serving on %s", addr)
	http.ListenAndServe(addr, nil)
	return nil
}

func main() {
	if len(os.Args) < 4 {
		usage()
		return
	}

	runCommand(loadConfigs(), os.Args[3], os.Args[4:])
}
