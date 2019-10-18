# PDB
```$xslt
    String path = "/tmp/pdb";
    PDBBuilder builder = new PDBBuilder();
    builder.path(path);
    this.pdb = builder.build();
    for(int i = 0; i < 1000; i ++){
         pdb.put(Bytes.toBytes(i), Bytes.toBytes(i));
    }
    pdb.get(Bytes.toBytes(1));
    pdb.scan(Bytes.toBytes(1),Bytes.toBytes(100));
    pdb.delete(Bytes.toBytes(1));
    pdb.clean();
```