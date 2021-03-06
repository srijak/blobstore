package main

import (
	"fmt"
	"os"
	l4g "log4go.googlecode.com/hg"
	. "github.com/srijak/blobstore"
	"io/ioutil"
	"flag"
)

var hostname = flag.String("h", "localhost", "host to connect to. Default: localhost")
var port = flag.Int("p", 8080, "port to connect to. Default: 8080")

func usage() {
	fmt.Println("\nUsage: ccli -h <host> -p <port> <command>")
	fmt.Println("Defaults:")
	fmt.Println(" host: localhost")
	fmt.Println(" port: 8080")
	fmt.Println("Commands:")
	fmt.Println(" put <file>\tput file on blobstore. Returns key.")
	fmt.Println(" get <key>\tget data associated with the given key. Prints out data to stdout.")
	fmt.Println("")
}

func runCommand(command string, rs IRemoteStore, args []string) {
	var err os.Error
	switch {
	case command == "put":
		err = put(rs, args)
	case command == "get":
		err = get(rs, args)
	default:
		usage()
	}
	if err != nil {
		l4g.Error(err)
	}
}

func put(rs IRemoteStore, args []string) os.Error {
	if len(args) < 1 {
		return os.NewError("Need atleast one file to put.")
	}

	filename := args[0]

	blob, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	var key string
	err = rs.Put(&blob, &key)
	fmt.Printf("Key: %s\n", key)
	return err
}

func get(rs IRemoteStore, args []string) os.Error {
	if len(args) < 1 {
		return os.NewError("Need atleast hash to retrieve.")
	}
	key := args[0]
	var blob []byte
	err := rs.Get(&key, &blob)
	if err != nil {
		return err
	}

	fmt.Printf("%s", blob)

	return nil
}
func main() {
	if len(os.Args) < 3 {
		usage()
		return
	}
	flag.Parse()

	rs, err := NewRemoteStore(*hostname, *port)
	if err != nil {
		l4g.Error(err.String())
		return
	}
	runCommand(flag.Arg(0), rs, flag.Args()[1:])
}
