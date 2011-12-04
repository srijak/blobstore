blobstored
----------

This is what you use to run/manage a blobserver.  



#### To start up a blobserver:

    bsd -c configfile serve

#### To add a new vnode on the localmachine starting at offset -121 [0]

    bsd -c configfile add-vnode -121

#### To remove the vnode with offset from the local machine [0]

    bsd -c configfile rm-vnode -121

#### To list all available vnodes on the cluster

    bsd -c configfile ls-vnodes  
   
 
<br/>
<br/>
<br/>
<br/>
<br/>


[0] These actions just remove the vnodes from zookeeper. You actual data is
still available under RootDir( as defined in your config file)/<vnodeoffset>

