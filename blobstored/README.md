blobstored
----------

This is what you use to run/manage a blobserver.  



#### To start up a blobserver:

    bsd -c configfile serve

#### Add vnode at offset -121 [0]

    bsd -c configfile add-vnode -121

#### Remove vnode with offset [0]

    bsd -c configfile rm-vnode -121

#### To list all available vnodes on the cluster

    bsd -c configfile ls-vnodes  
   
 
<br/>
<br/>
<br/>
<br/>
<br/>


[0] These actions just add/remove the vnode to/from zookeeper. You actual data is
still available under RootDir( as defined in your config file)/<vnodeoffset>.
Also, the vnode's primary location will be the local machine.

