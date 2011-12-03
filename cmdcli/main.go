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
	fmt.Printf("Usage: ccli <port> <command>\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf(" put <file>\tput file on blobstore. Return hash.\n")
	fmt.Printf(" get <hash>\tget data associated with the given hash.\n")
	fmt.Printf("\n")
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
