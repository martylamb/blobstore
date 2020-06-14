# blobstore

## A simple API for providing a content-addressable, local disk-backed data store.

A `BlobStore` provides methods for storing and retrieving arbitrary data ("blobs") of any size.  Blobs are accessed via a Blob ID constructed from a MessageDigest of the blob.  The underlying storage mechanism for the blobs is managed by the BlobStore implementation.

Here's an example showing the basic operations:

```java
try (BlobStore bs = DigestBlobStore.sha256(Paths.get("~/blobstore-example"))) {
    // add a byte array directly...
    byte[] b = "This is a test".getBytes(StandardCharsets.UTF_8);
    Blob blob = bs.add(b);
    System.out.format("Added %s%n", blob);

    // ...or a file
    Path p = Paths.get("README.md");
    Blob blob2 = bs.add(p);
    System.out.format("Added %s%n", blob2);

    // ...or an InputStream
    Blob blob3 = bs.add(Files.newInputStream(p));
    System.out.format("Added %s%n", blob3);

    // of course you can retrieve them as well
    String id = blob.id(); // this is the first Blob we added above
    Optional<Blob> oBlob4 = bs.get(id); // optional, may not have been found
    Blob blob4 = oBlob4.get();
    System.out.format("Retrieved %s%n", blob4);

    // and read back the data
    byte[] buf = new byte[(int) blob4.size()];
    new DataInputStream(blob4.getInputStream()).readFully(buf);
    System.out.format("Retrieved data is [%s]%n", new String(buf, StandardCharsets.UTF_8));
}

```

And the output:

```console
Added Blob id=c7be1ed902fb8dd4d48997c6452f5d7e509fbcdbe2808b16bcf4edce4c07d14e size=14
Added Blob id=03af2fcd80f6d1f910a111f58256ca252b487113e261ea4f9af0e5e7f3f408d3 size=2196
Added Blob id=03af2fcd80f6d1f910a111f58256ca252b487113e261ea4f9af0e5e7f3f408d3 size=2196
Retrieved Blob id=c7be1ed902fb8dd4d48997c6452f5d7e509fbcdbe2808b16bcf4edce4c07d14e size=14
Retrieved data is [This is a test]
```
