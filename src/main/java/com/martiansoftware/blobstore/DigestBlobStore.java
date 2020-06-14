/*
   Copyright 2015-2020, Marty Lamb

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package com.martiansoftware.blobstore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.martiansoftware.hex.Hex;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.validation.Hope;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A BlobStore implementation that arranges Blobs in a variable-depth
 * filesystem layout according to the hexadecimal encoding of a MessageDigest
 * of the Blob data.  Each directory in the Blob filesystem may contain up to a
 * specified number of Blob files, with any additional files stored in deeper
 * subdirectories named for the (hexadecimal) next byte of the MessageDigest.
 * 
 * This class is thread-safe; BlobStore IO operations are single-threaded
 * (managed internally).  Copying Blob data into the BlobStore (via the add()
 * methods) is NOT single-threaded but is still thread-safe.
 * 
 * @author <a href="http://martylamb.com">Marty Lamb</a>
 */
public class DigestBlobStore implements BlobStore, AutoCloseable {

    // 256 subdirs, 254 blobs, ".", and ".." add up to 512 entries.  Testing
    // on a single computer (read: NOT RIGOROUS) showed this to be a decent
    // balance between depth and directory read speed.
    static final int DEFAULT_MAXBLOBS_PER_DIRECTORY = 254;
        
    private static final Logger LOG = LoggerFactory.getLogger(DigestBlobStore.class);
    static final String BLOBCOUNT_COUNTER_NAME = "blobCount";
    static final String BYTECOUNT_COUNTER_NAME = "byteCount";
    
    private final MetricRegistry _metrics = new MetricRegistry();

    private final IO _io = new IO(_metrics);
    private final Path _dir, _blobRoot, _incoming; // BlobStore parent dir, Blob dir, and temporary incoming Blob workspace
    private final Counter _blobCount, _byteCount;
    private final Object _lock = new Object();
    private final int DIGEST_SIZE_BYTES; // varies depending upon which MessageDigest algorithm is used
    private final String DIGEST_ALGORITHM;
    private final long MAX_BLOBS_PER_DIRECTORY; // configurable in case that's useful?
    private final AtomicLong _incomingBlobCount = new AtomicLong(0); // used for genering unique temporary incoming Blob filenames.  Blobs can be received on multiple threads.
    private boolean _closed = false;
    
    /**
     * Creates a new DigestBlobStore.
     * 
     * @param path the Path of the directory to use as a DigestBlobStore.  This
     * directory will be created if it does not exist.  Only the DigestBlobStore
     * should make any changes to the contents of this directory or its
     * subdirectories.
     * @param digestAlgorithm the name of the MessageDigest algorithm to use
     * (via MessageDigest.newInstance(digestAlgorithm))
     * @param maxBlobsPerDirectory the maximum number of Blobs any directory may
     * contain before additional subdirectories are used (default: 254)
     * @throws IOException if an error occurs while accessing the backing store
     */
    public DigestBlobStore(Path path, String digestAlgorithm, int maxBlobsPerDirectory) throws IOException {
        LOG.info("{}({},{},{})", DigestBlobStore.class.getName(), path, digestAlgorithm, maxBlobsPerDirectory);
        _dir = _io.ensureDirectoryExists(path);
        _blobRoot = _io.ensureDirectoryExists(_dir.resolve("blobs"));        
        _incoming = _io.ensureDirectoryExists(_dir.resolve("incoming"));
        _metrics.register(BLOBCOUNT_COUNTER_NAME, _blobCount = new Counter());
        _metrics.register(BYTECOUNT_COUNTER_NAME, _byteCount = new Counter());
        
        DIGEST_ALGORITHM = digestAlgorithm;
        MAX_BLOBS_PER_DIRECTORY = Hope.that(maxBlobsPerDirectory).named("maxBlobsPerDirectory").isTrue(mbpd -> mbpd > 0, "maxBlobsDirectory must be greater than zero").value();
        DIGEST_SIZE_BYTES = newDigest().getDigestLength();
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                try { 
                    synchronized(_lock) {
                        if (!_closed) {
                            LOG.debug("closing DigestBlobStore via shutdown hook");
                            close();
                        } 
                    } 
                } catch (IOException ignored) {}
            }
        });

        blobRoot().deepScanAndDedupe();
    }

    /**
     * Creates a new DigestBlobStore at the specified Path using the SHA-256
     * MessageDigest and the default maximum number of Blobs per directory
     * @param path the Path of the directory to use as a DigestBlobStore.  This
     * directory will be created if it does not exist.  Only the DigestBlobStore
     * should make any changes to the contents of this directory or its
     * subdirectories.
     * @return a new DigestBlobStore at the specified Path using the SHA-256
     * MessageDigest and the default maximum number of Blobs per directory
     * @throws IOException if an error occurs while accessing the backing store
     */
    public static DigestBlobStore sha256(Path path) throws IOException {
        // note: SHA-256 is required in all implementations of the JVM
        return new DigestBlobStore(path, "SHA-256", DEFAULT_MAXBLOBS_PER_DIRECTORY);
    }
    
    /**
     * Creates a new DigestBlobStore at the specified Path using the SHA-1
     * MessageDigest and the default maximum number of Blobs per directory
     * @param path the Path of the directory to use as a DigestBlobStore.  This
     * directory will be created if it does not exist.  Only the DigestBlobStore
     * should make any changes to the contents of this directory or its
     * subdirectories.
     * @return a new DigestBlobStore at the specified Path using the SHA-256
     * MessageDigest and the default maximum number of Blobs per directory
     * @throws IOException if an error occurs while accessing the backing store
     */
    public static DigestBlobStore sha1(Path path) throws IOException {
        // note: SHA-1 is required in all implementations of the JVM
        return new DigestBlobStore(path, "SHA-1", DEFAULT_MAXBLOBS_PER_DIRECTORY);
    }

    /**
     * Creates a new DigestBlobStore at the specified Path using the MD5
     * MessageDigest and the default maximum number of Blobs per directory
     * @param path the Path of the directory to use as a DigestBlobStore.  This
     * directory will be created if it does not exist.  Only the DigestBlobStore
     * should make any changes to the contents of this directory or its
     * subdirectories.
     * @return a new DigestBlobStore at the specified Path using the SHA-256
     * MessageDigest and the default maximum number of Blobs per directory
     * @throws IOException if an error occurs while accessing the backing store
     */
    public static DigestBlobStore md5(Path path) throws IOException {
        // note: SHA-1 is required in all implementations of the JVM
        return new DigestBlobStore(path, "MD5", DEFAULT_MAXBLOBS_PER_DIRECTORY);
    }

    /**
     * @see BlobStore#blobCount()
     */
    @Override
    public long blobCount() {
        return _blobCount.getCount();
    }

    /**
     * @see BlobStore#byteCount()
     */
    @Override
    public long byteCount() {
        return _byteCount.getCount();
    }

    /**
     * @see BlobStore#get(java.lang.String)
     */
    @Override
    public Optional<Blob> get(String id) throws IOException {
        BlobReference br = new BlobReference(id);
        synchronized(_lock) {
            ensureOpen();
            return blobRoot().get(br);
        }
    }

    /**
     * @see BlobStore#add(java.io.InputStream) 
     */
    @Override
    public Blob add(InputStream sourceStream) throws IOException {
        try(IncomingBlob incomingBlob = new IncomingBlob(sourceStream)) {
            BlobReference br = new BlobReference(incomingBlob.digest());
            synchronized(_lock) {
                ensureOpen();
                return blobRoot().add(br, incomingBlob);
            }
        }
    }

    /**
     * @see BlobStore#add(java.nio.file.Path) 
     */
    @Override
    public Blob add(Path sourceFile) throws IOException {
        return add(Files.newInputStream(sourceFile));
    }

    /**
     * @see BlobStore#add(byte[])
     */
    @Override
    public Blob add(byte[] sourceBytes) throws IOException {
        return add(new ByteArrayInputStream(sourceBytes));
    }

    /**
     * @see BlobStore#delete(java.lang.String)
     */
    @Override
    public boolean delete(String id) throws IOException {
        BlobReference br = new BlobReference(id);
        synchronized(_lock) {
            ensureOpen();
            return blobRoot().delete(br);
        }
    }
    
    private void ensureOpen() throws IOException {
        if (_closed) throw new IOException("DigestBlobStore has been closed.");
    }
    
    /**
     * @see BlobStore#close()
     */
    @Override
    public void close() throws IOException {
        synchronized(_lock) {
            if (!_closed) {                
                try {
                    LOG.debug("cleaning up incoming directory {}", _incoming);
                    _io.recursiveDeleteDirectory(_incoming, true);
                } catch (IOException e) {
                    LOG.error("unable to remove incoming directory {}: {}", _incoming, e.getMessage());
                    throw(e);
                }
                _closed = true;
            }
        }
    }

    /**    
     * Returns the Path of the directory used as a DigestBlobStore.  Only the
     * DigestBlobStore should make any changes to the contents of this directory
     * or its subdirectories.
     * @return the Path of the directory used as a DigestBlobStore.  Only the
     * DigestBlobStore should make any changes to the contents of this directory
     * or its subdirectories.
     */
    public Path getDirectory() {
        return _dir;
    }
    
    // get the top-level BlobDirectory.  Maybe add caching here if needed in the future?
    private BlobDirectory blobRoot() throws IOException {
        return new BlobDirectory(_blobRoot, "");
    }
    
    // "safely" get an instance of the configured digest algorithm.  This is called
    // from the constructor so it should fail immediately if an invalid algorithm
    // is specified (and if successful in the constructor, should then consistently
    // succeed)
    private MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    
    // useful for converting digests to (String) ids and vice-versa
    class BlobReference {
        // string to byte conversion, etc
        final String id;
        final byte[] digest;

        BlobReference(String id) {
            this.digest = IO.parseHex(id);
            Hope.that(digest).named("id").isTrue((b) -> b.length == DIGEST_SIZE_BYTES, "ID must be %d-char hexadecimal", DIGEST_SIZE_BYTES * 2);
            this.id = Hex.encode(digest);
        }

        BlobReference(byte[] digest) {
            Hope.that(digest).named("digest").isTrue((b) -> b.length == DIGEST_SIZE_BYTES, "digest must be %d bytes long", DIGEST_SIZE_BYTES);
            this.digest = Arrays.copyOf(digest, digest.length);
            this.id = Hex.encode(digest);
        }

        @Override public String toString() {
            return id;
        }
        
        @Override public int hashCode() {
            int hash = 3;
            hash = 29 * hash + Objects.hashCode(this.id);
            hash = 29 * hash + java.util.Arrays.hashCode(this.digest);
            return hash;
        }

        @Override public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            final BlobReference other = (BlobReference) obj;
            if (!Objects.equals(this.id, other.id)) return false;
            return (java.util.Arrays.equals(this.digest, other.digest));
        }

    }
    
    // blob subdirectories are always two-character lowercase hexadecimal
    private static final Matcher SUBDIR_MATCHER = Pattern.compile("^[0-9a-f]{2}$").matcher("");
    
    // utility for dealing with individual layers of the Blob filesystem hierarchy
    class BlobDirectory {
        
        private final Path _dir; // the path of this particular layer
        private final int _depth; // how "deep" in the Blob filesystem hierarchy are we?  Top level is 0, next is 1, ...
        private final String _prefix; // what is the (hex) prefix that all Blob files must begin with in this directory?  This is the concatenation of the (hex) subdirectory names leading to this directory.  Top level has no prefix (any Blobs may be stored at the top level).
        private final Matcher _blobFileMatcher;  // used to determine which files are properly-named Blob files (other, unexpected files are ignored)
        private final Set<Path> _blobs, _subdirs; // stores the Blob files and subdirectories that were found when reading this directory
        private boolean _readDir = false; // have we already read this directory?  ("read" in the variable name is past tense)
        
        BlobDirectory(Path dir, String prefix) throws IOException {
            _dir = dir;
            _depth = prefix.length() / 2;
            _prefix = prefix;
            _blobFileMatcher = Pattern.compile( // hast to start with the correct prefix, AND have the correct remaining number of hex digits and extension
                                String.format("^%s[0-9a-f]{%d}\\.blob$", 
                                                _prefix, 
                                                DIGEST_SIZE_BYTES * 2 - _prefix.length()))
                                .matcher("");
            _blobs = new HashSet<>();
            _subdirs = new HashSet<>();
        }
        
        
        Blob add(BlobReference br, IncomingBlob incomingBlob) throws IOException {
            readDir();
            Path p = resolve(br);

            // already exists?  just return the existing blob.
            if (_blobs.contains(p)) return new DigestBlob(br, p);
        
        
            // current directory is not full but we haven't found the blob?  then add it here.
            if (!isFull()) {
                _blobs.add(incomingBlob.moveTo(p));                
                Blob result = new DigestBlob(br, p);
                _metrics.getCounters().get(BLOBCOUNT_COUNTER_NAME).inc();
                _metrics.getCounters().get(BYTECOUNT_COUNTER_NAME).inc(result.size());
                
                // if blob already exists below, then delete it from lower directories
                // because it just got a promotion due to a higher-level directory vacancy
                BlobDirectory child = descend(br, false);
                if (child != null) child.delete(br);

                return result;
            }

            // still not found and haven't added to a non-full directory?  then descend into the next dir.
            return descend(br, true).add(br, incomingBlob);
        }    

        Optional<Blob> get(BlobReference br) throws IOException {
            readDir();
            Path p = resolve(br);
            if (_blobs.contains(p)) return Optional.of(new DigestBlob(br, p));
            BlobDirectory child = descend(br, false);
            return child == null ? Optional.empty() : child.get(br);
        }
        
        boolean delete(BlobReference br) throws IOException {
            boolean result = false;
            readDir();
            Path p = resolve(br);
            if (_blobs.contains(p)) {
                long size = 0;
                try {
                    size = _io.size(p);
                } catch (IOException e) {
                    LOG.error("unable to determine size of " + p + ".  Byte count of BlobStore might now be unreliable.", e);
                }
                
                if (_io.deleteIfExists(p)) {
                    _blobs.remove(p);
                    _metrics.getCounters().get(BLOBCOUNT_COUNTER_NAME).dec();
                    _metrics.getCounters().get(BYTECOUNT_COUNTER_NAME).dec(size);
                    
                    if (_depth > 0) _io.deleteDirIfEmpty(_dir); // don't delete top-level blob dir
                    result = true;
                }
            }
            BlobDirectory child = descend(br, false);
            if (child != null) result |= child.delete(br);
            return result;
        }

        // get the next-level BlobDirectory for the given BlobReference, optionally creating if it does not exist
        BlobDirectory descend(BlobReference br, boolean create) throws IOException {
            String nextPathElement = String.format("%02x", br.digest[_depth]);
            Path p = _dir.resolve(nextPathElement);
            if (create) _io.ensureDirectoryExists(p);
            if (_readDir) _subdirs.add(p);
            return (_io.isDirectory(p)) ? new BlobDirectory(p, _prefix + nextPathElement) : null;
        }        

        // read the directory contents and note any Blob files or subdirectories, ignoring everything else
        void readDir() throws IOException {
            if (!_readDir) {
                try (DirectoryStream<Path> stream = _io.newDirectoryStream(_dir)) {
                    for (Path f : stream) {
                        String fname = f.getFileName().toString();                
                        if (isValidSubdirName(fname) && Files.isDirectory(f)) {
                            _subdirs.add(f);
                        } else if (isValidFileName(fname) && Files.isRegularFile(f)) {
                            _blobs.add(f);
                        }
                    }
                }        
                _readDir = true;
            }
        }

        // only call this on the top-level blob directory
        void deepScanAndDedupe() throws IOException {
            // reset counters to zero - they will be updated in the recursive deepScanAndDedupe()
            _blobCount.dec(_blobCount.getCount());
            _byteCount.dec(_byteCount.getCount());
            deepScanAndDedupe(new java.util.HashSet<>(0));
        }
        
        // inputs:  a Set<BlobReference> (initially empty) and blobcount/bytecount are initialized to ZERO before calling
        // outputs: (side effects: duplicates are deleted, byte/blob counts are updated, unexpected files and directories are warned in log)
        // invariant: once deepscan is called, ALL previously instantiated blobdirectories are invalid!  perhaps a deepscan and instantiation timestamps are needed?
        // single-threaded, as are all io operations in DigestBlobStore
        private void deepScanAndDedupe(Set<BlobReference> parent) throws IOException {
            Set<BlobReference> blobRefs = new ChainedHashSet<>(parent);
            
            readDir();
            
            for (Path blobPath : _blobs) {
                BlobReference blobRef = new BlobReference(blobPath.getFileName().toString().substring(0, DIGEST_SIZE_BYTES * 2));
                if (blobRefs.contains(blobRef)) {                    
                    // parent already has this blob, so DELETE from this dir
                    LOG.warn("deleting duplicate blob {}", blobPath.getFileName());
                    _io.deleteIfExists(blobPath);
                } else {
                    blobRefs.add(blobRef);
                    _blobCount.inc();
                    _byteCount.inc(_io.size(blobPath));
                }
            }
            
            for (Path subdir : _subdirs) {
                BlobDirectory bd = new BlobDirectory(subdir, _prefix + subdir.getFileName().toString());
                bd.deepScanAndDedupe(blobRefs);
            }
            
            if (_depth > 0) _io.deleteDirIfEmpty(_dir);
        }

        Path resolve(BlobReference br) throws IOException {
            return _dir.resolve(blobFilenameFor(br));
        }        

        String blobFilenameFor(BlobReference br) {
            return br.id + ".blob";
        }        

        boolean isValidFileName(String name) {
            _blobFileMatcher.reset(name);
            return _blobFileMatcher.matches();
        }

        boolean isValidSubdirName(String name) {
            SUBDIR_MATCHER.reset(name);
            return SUBDIR_MATCHER.matches();
        }
        
        boolean isFull() {
            return _blobs.size() >= MAX_BLOBS_PER_DIRECTORY;
        }

        // helper class for deep scanning, to see if any parent directories
        // contain the same things as the current directory.
        // only useful for contains() method.  others (isEmpty, iterator, etc. have
        // NO awareness of the parents and will not produce correct results)
        class ChainedHashSet<T> extends HashSet<T> {
            private final Set<T> _parent;

            ChainedHashSet(Set<T> parent) {
                _parent = parent;
            }

            @Override public boolean contains(Object o) {
                return super.contains(o) ? true : _parent.contains(o);
            }
        }        
    }
    
    // helper for temporarily storing Blob content as it is added to the DigestBlobStore and computing its MessageDigest
    class IncomingBlob implements AutoCloseable {

        private final Path _file;
        private final long _size;
        private final byte[] _digest;

        IncomingBlob(InputStream in) throws IOException {
            String fname = String.format("incoming-%d.tmp", _incomingBlobCount.getAndIncrement());
            _file = _incoming.resolve(fname);

            try (DigestOutputStream out = new DigestOutputStream(new BufferedOutputStream(_io.newOutputStream(_file)), newDigest())) {
                _size = _io.copyAndClose(in, out);
                _digest = out.getMessageDigest().digest();
            } catch (IOException e) {
                LOG.error("error receiving blob", e.getMessage());
                _io.deleteIfExists(_file);
                throw(e);
            }

        }

        public byte[] digest() { return _digest; }
        public long size() { return _size; }

        Path moveTo(Path destFile) throws IOException {
            LOG.debug("move incoming blob: [{}] -> [{}]", _file.toAbsolutePath(), destFile.toAbsolutePath());

            _io.createDirectories(destFile.getParent());
            _io.moveAtomic(_file, destFile);
            return destFile;
        }

        @Override
        public void close() throws IOException {
            _io.deleteIfExists(_file);
        }
    }
    
    // our implementation of the Blobs we will return
    class DigestBlob implements Blob {
        private final Path _path;
        private final String _id;
        private final long _size;
        
        DigestBlob(BlobReference br, Path path) throws IOException {
            _path = path;
            _id = br.id;
            _size = _io.size(path);
        }
        
        @Override public String id() { return _id; }
        @Override public long size() { return _size; }

        @Override
        public InputStream getInputStream() throws IOException {
            return Files.newInputStream(_path);
        }

        @Override
        public String toString() {
            return description();
        }
    }
    
}
