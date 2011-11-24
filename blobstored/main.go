package main

import (
	"http"
	l4g "log4go.googlecode.com/hg"
	"rpc"
	bs "github.com/srijak/blobstore"
)

func main() {
	b := bs.NewBlobStore()
	rpc.Register(b)
	rpc.HandleHTTP()
	l4g.Info("Serving at 8080")
	http.ListenAndServe(":8080", nil)
}
