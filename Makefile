include $(GOROOT)/src/Make.inc

TARG=blobstore
GOFILES=\
	blobstore.go\
	utils.go\
	keyspace.go\

include $(GOROOT)/src/Make.pkg
