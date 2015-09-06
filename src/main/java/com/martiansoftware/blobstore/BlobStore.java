/*
 * Copyright 2015 Martian Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.martiansoftware.blobstore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A simple API for providing a content-addressable, local disk-backed data
 * store.
 * 
 * <p>A BlobStore arranges its data similar to a git repository but does not
 * provide any versioning, transactions, or similar source control-related
 * functionality (but those could be added on top of BlobStore).</p>
 * 
 * <p>By default, BlobStore uses SHA-1 as its MessageDigest to create content
 * addresses (called "Refs" in this API).  You can supply alternative
 * MessageDigest names or digests, or even skip the MessageDigest altogether
 * and manually specify Refs for your data (treating the BlobStore much like a
 * HashMap - e.g. to use two parallel BlobStores, one using digests and the
 * other containing metadata with the same Ref as its associated data Blob).</p>
 * 
 * <p>Blobs are accessed via Refs, which the BlobStore uses internally to locate
 * the backing files.  Blobs provide access to the actual data.</p>
 * 
 * <p>When content is added to a BlobStore, it is first copied into a BlobStore
 * staging directory, and is then moved to the BlobStore (if necessary) using
 * the ATOMIC_MOVE CopyOption.</p>
 * 
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class BlobStore {

    // TODO: add a gc() method to clean up any partially-staged files of a
    //       certain age?
    // TODO: allow completely arbitrary String-based Refs with base64 encoding?
    //       (might require allowing arbitrary-length Refs within a BlobStore,
    //       or else encoding Strings to a fixed length)

    /** The top level directory of this BlobStore */
    final Path _root;
    
    /** The data directory in the BlobStore -- where the Blobs themselves live. */
    final Path _data;
    
    /** The staging directory in the BlobStore -- where new content lives temporarily
     *  prior to moving into the store */
    final Path _staging;
    
    /** A directory storing data that determines BlobStore internal behavior */
    final Path _flags;
    
    /** If this file exists, then this BlobStore has had a put() method called
     *  at least once in its lifetime.  If this is the case, then blob data must
     *  always be overwritten regardless of whether its Ref already points to
     *  data, since we can't count on the MessageDigest to indicate that the
     *  content is identical. */
    final Path _putModeFlag;
    
    /** Where the MessageDigests come from when add()ing content */
    final Supplier<MessageDigest> _digestSupplier;
    
    /** How long is a digest (and thus a Ref?) */
    final int _digestLengthBytes;
    
    /** Used to prevent simultaneous write access to the BlobStore's data */
    final FileChannel _lockChannel;
    
    /** If true, then add() methods may be used.  Only false if a fixed ref
     *  length is used instead of a MessageDigest. */
    final boolean _allowAdd;
    
    /** Runtime version of _putModeFlag */
    final AtomicBoolean _putMode;
    
    /**
     * Creates a new BlobStore at the specified filesystem location, using SHA-1
     * as its MessageDigest
     * 
     * @param path the disk location for the BlobStore
     * @throws IOException 
     */
    public BlobStore(Path path) throws IOException {
        this(path, "SHA-1");
    }
    
    /**
     * Creates a new BlobStore at the specified filesystem location, using the
     * specified digest name to obtain MessageDigest instances
     * 
     * @param path the disk location for the BlobStore
     * @param digestName the name of the MessageDigest algorithm to use.
     * @throws IOException 
     * @throws RuntimeNoSuchAlgorithmException if digestName is invalid
     */
    public BlobStore(Path path, String digestName) throws IOException {
        this(path, new NamedDigestSupplier(digestName));
    }
    
    /**
     * Creates a new BlobStore at the specified filesystem location, using the
     * specified Supplier<MessageDigest> to obtain MessageDigest instances
     * 
     * @param path the disk location for the BlobStore
     * @param digestSupplier the source for all MessageDigest instances used
     * @throws IOException 
     */
    public BlobStore(Path path, Supplier<MessageDigest> digestSupplier) throws IOException {
        this(path, true, digestSupplier, 0);
    }
    
    /**
     * Creates a new BlobStore that does not digest any data and therefore does
     * not support the add() methods.  Only put() methods may be used on a
     * BlobStore instantiated by this constructor.
     * 
     * @param path the disk location for the BlobStore
     * @param fixedRefLength the length to enforce for all Refs (must be greater
     * than or equal to 2)
     * @throws IOException 
     */
    public BlobStore(Path path, int fixedRefLength) throws IOException {
        this(path, false, null, fixedRefLength);
    }
    
    private BlobStore(Path path, boolean useSupplier, Supplier<MessageDigest> digestSupplier, int fixedRefLength) throws IOException {
        _root = ensureDirectory(path);
        _data = ensureDirectory(_root.resolve("data"));
        _staging = ensureDirectory(_root.resolve(".staging"));
        _flags = ensureDirectory(_root.resolve(".flags"));
        _putModeFlag = _flags.resolve("put");
        _putMode = new AtomicBoolean(Files.exists(_putModeFlag));
        _digestSupplier = digestSupplier;
        _allowAdd = useSupplier;
        if (useSupplier) {
            _digestLengthBytes = _digestSupplier.get().getDigestLength();
        } else {
            if (fixedRefLength < 2) throw new IllegalArgumentException("Fixed Ref length must be >= 2");
            _digestLengthBytes = fixedRefLength;
        }
        _lockChannel = FileChannel.open(_root.resolve(".lock"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);                
    }
    
    /**
     * Copies the specified byte array to the BlobStore and digests it to compute
     * its Ref.
     * 
     * @param b the bytes to add
     * @return the newly added Blob
     * @throws IOException 
     */
    public Blob add(byte[] b) throws IOException {
        return put(null, new ByteArrayInputStream(b));
    }
    
    /**
     * Copies the specified File to the BlobStore and digests it to compute its
     * Ref.
     * 
     * @param f the file to add
     * @return the newly added Blob
     * @throws IOException 
     */
    public Blob add(File f) throws IOException {
        return put(null, new FileInputStream(f));
    }
    
    /**
     * Copies data from the specified InputStream to the BlobStore and digests it
     * to compute its Ref.
     * 
     * @param in the InputStream to copy
     * @return the newly added Blob
     * @throws IOException 
     */
    public Blob add(InputStream in) throws IOException {
        return put(null, in);
    }
    
    /**
     * Adds the specified File to the BlobStore at a manually-specified Ref.
     * The file is not digested unless the manually-specified Ref is null,
     * in which case this is exactly equivalent to calling add(File).
     * 
     * @param ref the Ref at which the File will be added
     * @param f the file to add
     * @return the newly added Blob
     * @throws IOException 
     */
    public Blob put(Ref ref, File f) throws IOException {
        return put(ref, new FileInputStream(f));
    }
    
    /**
     * Copies the specified byte array to the BlobStore at a manually-specified Ref.
     * The byte array is not digested unless the manually-specified Ref is null,
     * in which case this is exactly equivalent to calling add(byte[]).
     * 
     * @param ref the Ref at which the byte array will be added
     * @param b the bytes to add
     * @return the newly added Blob
     * @throws IOException 
     */    
    public Blob put(Ref ref, byte[] b) throws IOException {
        return put(ref, new ByteArrayInputStream(b));
    }
    
    /**
     * Copies data from the specified InputStream to the BlobStore at a manually-specified
     * Ref.  The data is not digested unless the manually-specified Ref is null,
     * in which case this is exactly equivalent to calling add(InputStream)
     * to compute its Ref.
     * 
     * @param ref the Ref at which the byte array will be added
     * @param in the InputStream to copy
     * @return the newly added Blob
     * @throws IOException 
     */
    public Blob put(Ref ref, InputStream in) throws IOException {
        Path tmpFile = Files.createTempFile(_staging, "prefix", "suffix");
        try {
            if (ref == null) {
                checkAdd();
                MessageDigest md = _digestSupplier.get();
                copyAndClose(in, new DigestOutputStream(Files.newOutputStream(tmpFile), md));
                ref = new Ref(md.digest());
            } else {
                checkRef(ref);
                if (!_putMode.getAndSet(true)) Files.createFile(_putModeFlag);
                copyAndClose(in, Files.newOutputStream(tmpFile));
            }
            Path dest = _data.resolve(ref.toPath());
            ensureDirectory(dest.getParent());
            try(@SuppressWarnings("unused") FileLock lock = _lockChannel.lock()) {
                if (_putMode.get() || !Files.exists(dest)) {
                    // in put mode we always need to overwrite
                    Files.move(tmpFile, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            return new Blob(this, ref);
        } finally {
           Files.deleteIfExists(tmpFile);
        }        
    }

    public String digestName() {
        return (_digestSupplier == null) ? null : _digestSupplier.get().getAlgorithm();
    }
    
    private void checkAdd() {
        if (!_allowAdd) throw new UnsupportedOperationException("add() methods and put() methods with null Refs are not supported because no digest has been configured for this BlobStore.");
    }
    
    private void checkRef(Ref ref) {
        if (ref.length() != _digestLengthBytes)
            throw new IllegalArgumentException (
                String.format("Invalid Ref length: was %d but expected %d.", ref.length(), _digestLengthBytes));
    }
    
    /**
     * Deletes the Blob at the specified Ref.  No action is taken if the Blob
     * does not exist
     * 
     * @param ref the Ref to delete
     * @return the deleted Blob (which should now indicate no data)
     * @throws IOException 
     */
    public Blob delete(Ref ref) throws IOException {
        checkRef(ref);
        Path p = _data.resolve(ref.toPath());
        try(@SuppressWarnings("unused") FileLock lock = _lockChannel.lock()) {
            Files.deleteIfExists(p);
            FSUtils.deleteDirIfEmpty(p.getParent());
        }
        return new Blob(this, ref);
    }
    
    private void copyAndClose(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[32 * 1024];
        int bytesRead;
        while ((bytesRead = in.read(buf)) > 0) {
            out.write(buf, 0, bytesRead);
        }
        out.close();
        in.close();
    }
        
    /**
     * Returns the Blob at the specified Ref.  If no data exists at the
     * specified Ref, this will be indicated by the returned Blob.
     * 
     * @param ref the Ref of the Blob to return
     * @return the requested Blob
     */
    public Blob get(Ref ref) {
        checkRef(ref);
        return new Blob(this, ref);
    }
    
    /**
     * Returns the top-level directory location of this BlobStore
     * 
     * @return the top-level directory location of this BlobStore
     */
    public Path location() { return _root; }
    
    private Matcher hexMatcher(int byteCount) {
        return Pattern.compile("^[0-9a-f]{" + (byteCount * 2) + "}$").matcher("");
    }
    
    /**
     * Returns a Stream of all Blobs in this BlobStore
     * 
     * @return a Stream of all Blobs in this BlobStore
     * @throws IOException 
     */
    public Stream<Blob> stream() throws IOException {
        final Matcher parentMatcher = hexMatcher(1);
        final Matcher fileMatcher = hexMatcher(_digestLengthBytes - 1);
        return Files.walk(_data, 2)
                .filter(Files::isRegularFile)
                .filter(p -> fileMatcher.reset(p.getFileName().toString()).matches())
                .filter(p -> parentMatcher.reset(p.getParent().getFileName().toString()).matches())
                .map(p -> get(new Ref(p.subpath(p.getNameCount() - 2, p.getNameCount()))));
    }
    
    private Path ensureDirectory(Path p) throws IOException {
        if (!Files.exists(p)) Files.createDirectories(p); // throws its own exception
        if (!Files.exists(p)) throw new FileNotFoundException(p.toFile().getAbsolutePath());
        if (!Files.isDirectory(p)) throw new IOException(p + " already exists but is not a directory.");
        return p;
    }
    
    @Override public String toString() {
        return String.format("BlobStore %s using digest %s with digest length %d", _root.toAbsolutePath(), _digestSupplier.get().getAlgorithm(), _digestLengthBytes);
    }
    
    /**
     * Utility class to supply MessageDigest instances with a specific name
     */
    public static class NamedDigestSupplier implements Supplier<MessageDigest> {
        private final String _digestName;
        public NamedDigestSupplier(String digestName) { _digestName = digestName; }

        @Override public MessageDigest get() {
            try { 
                return MessageDigest.getInstance(_digestName);
            } catch (NoSuchAlgorithmException oops) { 
                throw new RuntimeNoSuchAlgorithmException(oops); 
            }
        }        
    }
    
    /**
     * Wraps around NoSuchAlgorithmException to eliminate need to check for it.
     */
    public static class RuntimeNoSuchAlgorithmException extends RuntimeException {
        RuntimeNoSuchAlgorithmException(NoSuchAlgorithmException e) {
            super(e);
        }
    }
    
}
