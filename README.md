# blobstore

## A simple API for providing a content-addressable, local disk-backed data store.

A `BlobStore` arranges its data similar to a git repository but does not provide any versioning, transactions, or similar source control-related functionality (although those could be added on top of BlobStore).

By default, `BlobStore` uses SHA-1 as its `MessageDigest` to create content addresses (called "`Ref`s" in this API).  You can supply alternative `MessageDigest` names or digests, or even skip the `MessageDigest` altogether and manually specify `Ref`s for your data (treating the `BlobStore` much like a `HashMap` - e.g. to use two parallel BlobStores, one using digests and the other containing metadata with the same Ref as its associated data Blob).

`Blob`s are accessed via `Ref`s, which the `BlobStore` uses internally to locate the backing files.  `Blob`s provide access to the actual data.
 
When content is added to a `BlobStore`, it is first copied into a `BlobStore` staging directory, and is then moved to the `BlobStore` (if necessary) using the ATOMIC_MOVE `CopyOption`.

Here's an example showing the basic operations: :+1:

```java
BlobStore bs = new BlobStore(Paths.get("/path/to/your/blobstore"));
System.out.println("Created " + bs);

// add a byte array directly...
byte[] b = "This is a test".getBytes();
Blob blob = bs.add(b);
System.out.println("Added " + blob);

// ...or a file
File f = new File("/path/to/my/file");
Blob blob2 = bs.add(f);
System.out.println("Added " + blob2);

// ...or an InputStream
Blob blob3 = bs.add(someInputStream);
System.out.println("Added " + blob3);

Ref ref = blob.ref();
System.out.println("Ref is " + ref);
System.out.println("Blob exists: " + blob.exists());
System.out.println("Blob size: " + blob.size());

Blob readback = bs.get(ref);
System.out.println("Read back " + readback.size() + " bytes");

// read Blob data back into a byte array.  You can just as easily copy it
// to a file via Blob.copyTo() or obtain an InputStream from the Blob
// via Blob.inputStream()
String s = new String(readback.readAllBytes());
System.out.println("Data read back: " + s);

readback.delete();
System.out.println("Deleted blob.");
System.out.println("Original blob data exists: " + blob.exists());
System.out.println("Readback blob data exists: " + readback.exists());
```

## Using with Maven

### Add the repository to your project:

```xml
<project>
	...
	<repositories>
		...
		<repository>
			<id>martiansoftware</id>
			<url>http://mvn.martiansoftware.com</url>
		</repository>
		...
	</repositories> 
	...
</project>
```

### Add the dependency to your project:
-----------------------------------

```xml
<project>
	...
	<dependencies>
		...
		<dependency>
			<groupId>com.martiansoftware</groupId>
			<artifactId>blobstore</artifactId>
			<version>0.1.0-SNAPSHOT</version>
			<scope>compile</scope>
		</dependency>
		...
	</dependencies>
	...
</project>
```
