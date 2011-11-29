include $(GOROOT)/src/Make.inc

TARG=blobstore
GOFILES=\
	utils.go\
	vnode.go\
	keyspace.go\
	replication_strategy.go\
	localstore.go\
	remotestore.go\
	blobstore.go\

include $(GOROOT)/src/Make.pkg
