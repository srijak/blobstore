package main

import (
	"fmt"
	"os"
	l4g "log4go.googlecode.com/hg"
	. "github.com/srijak/blobstore"
	"io/ioutil"
	"strconv"
)

func usage() {
	fmt.Printf("Usage: ccli <port> <command>\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf(" put <file>\tput file on blobstore. Return hash.\n")
	fmt.Printf(" get <hash>\tget data associated with the given hash.\n")
	fmt.Printf("\n")
}

func runCommand(command string, rs IRemoteStore, args []string) {
	var err os.Error
	fmt.Println("command: ", command)
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

	filename := args[1]

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
	key := args[1]
	var blob []byte
	err := rs.Get(&key, &blob)
	if err != nil {
		return err
	}

	fmt.Printf("%s", string(blob))

	return nil
}
func main() {
	if len(os.Args) < 3 {
		usage()
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		l4g.Error(err.String())
		usage()
		return
	}
	host, err := os.Hostname()
	if err != nil {
		l4g.Error(err.String())
		return
	}
	rs, err := NewRemoteStore(host, port)
	if err != nil {
		l4g.Error(err.String())
		return
	}
	runCommand(os.Args[2], rs, os.Args[2:])
}
