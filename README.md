# blobstore

## A simple API for providing a content-addressable, local disk-backed data store.

A `BlobStore` provides methods for storing and retrieving arbitrary data ("blobs") of any size.  Blobs are accessed via a Blob ID constructed from a MessageDigest of the blob.  The underlying storage mechanism for the blobs is managed by the BlobStore implementation.

Here's an example showing the basic operations:

```java
try (BlobStore bs = DigestBlobStore.sha256(Paths.get("target/blobstore-example"))) {
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

	// you can get some stats from the BlobStore - note that we
	// added the same README file twice above
	System.out.format("Total blobs: %d%n", bs.blobCount());
	System.out.format("Total bytes: %d%n", bs.byteCount());
	
	// of course you can retrieve them as well
	// this returns an Optional<Blob> because the requested Blob might not be found
	Optional<Blob> oBlob4 = bs.get("c7be1ed902fb8dd4d48997c6452f5d7e509fbcdbe2808b16bcf4edce4c07d14e");
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
Added Blob id=4ff3c898c31148f81be3fb0e1e33c0020992babcf5a5169a82a5961b95fd410f size=1854
Added Blob id=4ff3c898c31148f81be3fb0e1e33c0020992babcf5a5169a82a5961b95fd410f size=1854
Total blobs: 2
Total bytes: 1868
Retrieved Blob id=c7be1ed902fb8dd4d48997c6452f5d7e509fbcdbe2808b16bcf4edce4c07d14e size=14
Retrieved data is [This is a test]
```
