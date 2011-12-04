cmdcli:
----
Command line tool to put/get files to/from a blobstore cluster.


#### Usage:
    ccli -h <host> -p <port> <command>
      Defaults:
        host: localhost
        port: 8080
      Commands:
        put <file> put file on blobstore. Returns key.
        get <key>  get data associated with the given key. Prints out data to stdout.


