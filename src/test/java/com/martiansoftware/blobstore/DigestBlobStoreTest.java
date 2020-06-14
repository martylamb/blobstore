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

import com.google.common.base.Charsets;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.martiansoftware.hex.Hex;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mlamb
 */
public class DigestBlobStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(DigestBlobStoreTest.class);
    
    public static final Configuration[] FSTYPES = { Configuration.unix(), Configuration.windows(), Configuration.osX() };
    
    private static final int DIGEST_SIZE_BYTES_MD5 = 128/8;
    private static final int DIGEST_SIZE_BYTES_SHA1 = 160/8;
    private static final int DIGEST_SIZE_BYTES_SHA256 = 256/8;
    
    private Random _random = new Random();
    
    public DigestBlobStoreTest() {
    }

    // returns the utf-8 bytes of the given string
    private static byte[] bytesOf(String s) {
        return s.getBytes(Charsets.UTF_8);
    }
    
    // provides the hex form of the md5 digest of the given byte array
    private static String md5String(byte[] b) throws NoSuchAlgorithmException {
        MessageDigest d = MessageDigest.getInstance("MD5");
        return Hex.encode(d.digest(b));
    }
    
    // provides the hex form of the md5 digest of the given string (using utf-8 encoding)
    private String md5String(String s) throws NoSuchAlgorithmException {
        return md5String(bytesOf(s));
    }
    
    // creates a unique but deterministic string for test purposes
    private static String testString(int num) {
        return String.format("This is test number %d", num);
    }
    
    // counts regular files (not directories)
    private static long countFilesRecursively(Path dir) throws IOException {
         return Files.walk(dir).filter(p -> Files.isRegularFile(p)).count();
    }
    
    // adds enough blobs to fill the top level blob directory
    // returns number of BYTES written
    private static long createFullBlobDirectory(DigestBlobStore bs) throws IOException {
        long bytecount = 0;
        for (int i = 0; i < DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY; ++i) {
            byte[] b = bytesOf(testString(i));
            bytecount += b.length;
            bs.add(b);
        }        
        return bytecount;
    }
    
    // for each top-level blob, duplicate the blob to the appropriate subdirectory
    // returns number of duplicate blobs created
    private static long duplicateTopLevelBlobs(DigestBlobStore bs) throws IOException {
        Path blobDir = bs.getDirectory().resolve("blobs");
        long count = countFilesRecursively(blobDir);
        assertFalse(count == 0);
        AtomicLong dupeCount = new AtomicLong(0);
        Files.list(blobDir).filter(p -> Files.isRegularFile(p))
                .forEach(p -> {
                    Path subdir = blobDir.resolve(p.getFileName().toString().substring(0, 2));
                    try {
                        Files.createDirectories(subdir);
                        Files.copy(p, subdir.resolve(p.getFileName()));
                        dupeCount.incrementAndGet();
                    } catch (IOException e) {
                        fail(e.getMessage());
                    }
                });

        // make sure the dupes exist as expected
        assertEquals(2 * count, countFilesRecursively(blobDir));
        return dupeCount.get();
    }
    
    // sets up a a full top-level blobdir with a second dir layer containing
    // duplicates of all top-level blobs                               
    private static void createDuplicatedBlobDir(DigestBlobStore bs) throws IOException {
        createFullBlobDirectory(bs);
        duplicateTopLevelBlobs(bs);
        // make sure the dupes exist as expected
        assertEquals(2 * DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY,
                     countFilesRecursively(bs.getDirectory().resolve("blobs")));        
    }
        
    @Test public void testDeduplication() throws IOException {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg)) {
                createDuplicatedBlobDir(t.bs);
                
                try (DigestBlobStore deduped = new DigestBlobStore(t.bs.getDirectory(), "MD5", DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY)) {
                    assertEquals(DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY, deduped.blobCount());
                }
            }
        }
    }
    
    @Test public void tooManyBlobs() throws Exception {
        for (Configuration cfg : FSTYPES) {

            int biggerBlobsPerDir = 100;
            try (TestBlobStoreMd5 big = new TestBlobStoreMd5(cfg, biggerBlobsPerDir)) {                
                for (int i = 0; i < biggerBlobsPerDir; ++i) {
                    big.bs.add(bytesOf(testString(i)));
                }
                
                assertEquals(100, countFilesRecursively(big.path.resolve("blobs")));
                int smallerBlobsPerDir = 10;
                try (DigestBlobStore small = new DigestBlobStore(big.path, "MD5", smallerBlobsPerDir)) {
                    assertEquals(biggerBlobsPerDir, small.blobCount());
                    for (int i = 0; i < biggerBlobsPerDir; ++i) {
                        assertTrue(big.bs.get(md5String(testString(i))).isPresent());
                    }
                }
            }
        }        
    }
    
    @Test public void testEmptyDirsAreDeleted() throws Exception {
        for (Configuration cfg : FSTYPES) {

            int blobsPerDir = 10; // override DEFAULT_MAXBLOBS_PER_DIRECTORY so test completes in reasonable amount of time
            // make sure at least three layers of depth (force two layers, plus blobsPerDir additional blobs)
            int COUNT = blobsPerDir * (blobsPerDir + 2);
            String[] ids = new String[COUNT];

            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg, blobsPerDir)) {                
                
                for (int i = 0; i < COUNT; ++i) {
                    ids[i] = t.bs.add(bytesOf(testString(i))).id();
                }
                assertEquals(COUNT, t.bs.blobCount());
                
                for (String id : ids) assertTrue(t.bs.delete(id));
                assertEquals(0, t.bs.blobCount());
                
                Path p = t.bs.getDirectory().resolve("blobs");
                assertTrue(Files.isDirectory(p));
                assertEquals(0, countFilesRecursively(p));
            }
        }
    }
    
    @Test public void testAddWithLowerLayerDupe() throws Exception {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg)) {
                createDuplicatedBlobDir(t.bs);
                
                byte[] b12 = bytesOf(testString(12));
                String d12 = md5String(b12);
                Path p1 = t.bs.getDirectory().resolve("blobs").resolve(d12 + ".blob"); // top layer
                Path p2 = t.bs.getDirectory().resolve("blobs").resolve(d12.substring(0, 2)).resolve(d12 + ".blob"); // bottom layer
                assertTrue(Files.exists(p1));
                assertTrue(Files.exists(p2));
                
                // now delete the top level file, creating a vacancy in the top-level dir
                Files.delete(p1);
                assertFalse(Files.exists(p1));
                assertTrue(Files.exists(p2));
                
                // now add the content again...
                assertEquals(d12, t.bs.add(b12).id());

                // and ensure that it was added to the top layer and de-duped from the bottom
                assertTrue(Files.exists(p1));
                assertFalse(Files.exists(p2));                
            }
        }
    }

    @Test public void testDeletionWithDupes() throws Exception {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg)) {

                createDuplicatedBlobDir(t.bs);
                
                
                byte[] b1 = bytesOf(testString(13));
                String id1 = md5String(b1);
                
                assertTrue(t.bs.get(id1).isPresent());  // first check that it's there...
                assertTrue(Files.deleteIfExists(t.bs.getDirectory().resolve("blobs").resolve(id1 + ".blob"))); // make sure we delete the top entry
                assertTrue(t.bs.get(id1).isPresent());  // make sure it's STILL there (second-level instance now)
                assertTrue(t.bs.delete(id1));           // and STILL deletable
                assertFalse(t.bs.get(id1).isPresent());
                assertEquals(2 * DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY - 2,
                             countFilesRecursively(t.bs.getDirectory()));
                // TODO: verify bytecount and blobcount?
                
                byte[] b2 = bytesOf(testString(11));
                String id2 = md5String(b2);
                assertTrue(t.bs.delete(id2));          // make sure BOTH copies are deleted
                assertFalse(t.bs.get(id2).isPresent());
                assertEquals(2 * DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY - 4,
                             countFilesRecursively(t.bs.getDirectory()));
                // TODO: verify bytecount and blobcount?
                
            }
        }
    }
    
    @Test public void testBasicFunctionality() throws Exception {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg)) {
                long bc = 0;
                for (int i = 0; i < 1000; ++i) {
                    byte[] b = bytesOf(testString(i));
                    bc += b.length;
                    t.bs.add(b);
                }            

                assertEquals(1000, t.bs.blobCount());
                assertEquals(bc, t.bs.byteCount());
                
                for (int i = 55; i < 63; ++i) {        // add some duplicates
                    t.bs.add(bytesOf(testString(i)));
                }
                assertEquals(1000, t.bs.blobCount());  // and verify that the blob count...
                assertEquals(bc, t.bs.byteCount());    // ...and byte count are unchanged

                String test42 = md5String(testString(42));
                assertTrue(t.bs.get(test42).isPresent());
                assertEquals(22, t.bs.get(test42).get().size());
                
                // read back a value from the blobstore and verify its value
                byte[] sbuf = new byte[22];
                DataInputStream din = new DataInputStream(t.bs.get(test42).get().getInputStream());
                din.readFully(sbuf);
                String test42ReadBack = new String(sbuf, Charsets.UTF_8);
                assertEquals(testString(42), test42ReadBack);

                assertTrue(t.bs.delete(test42));             // delete a blob and verify it was there
                assertFalse(t.bs.get(test42).isPresent());   // make sure we can't retrieve it again
                assertFalse(t.bs.delete(test42));            // delete it again and verify it was NOT there
                
                assertEquals(999, t.bs.blobCount());         // now we have one blob less
                assertEquals(bc - 22, t.bs.byteCount());   // and 22 fewer bytes
            }
            
        }
    }

    // -- BlobDirectory --------------------------------------------------------
    @Test
    public void testBlobDirectory() throws IOException {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(cfg)) {
                
                Path dir = t.bs.getDirectory().resolve("testBlobDirectory");
                Files.createDirectories(dir);
                
                DigestBlobStore.BlobDirectory bd = t.bs.new BlobDirectory(dir, "d021");
                
                // BlobDirectory.isFull()
                assertFalse(bd.isFull()); // empty
                
                // BlobDirectory.isValidFileName()
                assertTrue(bd.isValidFileName( "d021d1bc9c32c0a73d8672c9783cb7bf.blob"));  // md5 of "testBlobDirectory"
                assertFalse(bd.isValidFileName("a021d1bc9c32c0a73d8672c9783cb7bf.blob"));  // wrong prefix (a at char 1)
                assertFalse(bd.isValidFileName("d021d1bc9c32c0a73d8672c9783cb7bf0.blob")); // too long (appended 0)
                assertFalse(bd.isValidFileName("d021d1bc9c32c0a73d8672c9783cb7b.blob"));   // too short (dropped f)
                assertFalse(bd.isValidFileName("d021d1bc9c32c0a73d8672c9783cb7bf"));       // no extension
                assertFalse(bd.isValidFileName("d021d1bc9c32c0a73d8672c9783cb7bf.BLOB"));  // uppercase extension
                assertFalse(bd.isValidFileName("D021d1bc9c32c0a73d8672c9783cb7bf.blob"));  // uppercase hex in first char
                assertFalse(bd.isValidFileName("d021D1bc9c32c0a73d8672c9783cb7bf.blob"));  // non hex at char 5
                assertFalse(bd.isValidFileName(" d021d1bc9c32c0a73d8672c9783cb7bf.blob")); // leading space
                assertFalse(bd.isValidFileName("d021d1bc9c32c0a73d8672c9783cb7bf.blob ")); // trailing space
                
                // BlobDirectory.isValidSubdirName()
                assertTrue(bd.isValidSubdirName( "aa"));
                assertFalse(bd.isValidSubdirName("aaa"));  // too long (appended a)
                assertFalse(bd.isValidSubdirName("a"));    // too short (dropped a)
                assertFalse(bd.isValidSubdirName("aA"));   // uppercase hex in second char
                assertFalse(bd.isValidSubdirName("xx"));   // non-hex
                assertFalse(bd.isValidSubdirName(" aa"));  // leading space
                assertFalse(bd.isValidSubdirName("aa "));  // trailing space

                // BlobDirectory.getPath() and (implicitly) BlobDirectory.blobFilenameFor()
                DigestBlobStore.BlobReference br = t.bs.new BlobReference("d021d1bc9c32c0a73d8672c9783cb7bf");
                assertEquals(dir.resolve("d021d1bc9c32c0a73d8672c9783cb7bf.blob"), bd.resolve(br));
                
            }
        }
    }

    // -- IncomingBlob ---------------------------------------------------------
    
    @Test
    public void testIncomingBlob() throws IOException {
        for (Configuration cfg : FSTYPES) {
            try (TestBlobStoreMd5 t = new TestBlobStoreMd5(Configuration.unix())) {
                String s = "testIncomingBlob";  // md5: 3f900831ce64970114f3bd1cda6f4d66
                DigestBlobStore.IncomingBlob ib = t.bs.new IncomingBlob(new ByteArrayInputStream(bytesOf(s)));
                assertEquals(16, ib.size());    // string length
                assertArrayEquals(IO.parseHex("3f900831ce64970114f3bd1cda6f4d66"), ib.digest());

                Path destFile = t.bs.getDirectory().resolve("testIncomingBlob.dat");
                ib.moveTo(destFile);
                assertEquals(16, Files.size(destFile));
                
                ib.close();
                
                byte[] b = bytesOf("Now let's force an exception");
                FilterInputStream badStream = new FilterInputStream(new ByteArrayInputStream(b)) {
                    @Override public int read() throws IOException {
                        throw new IOException("testIncomingBlob forced exception");
                    }
                    @Override public int read(byte[] b) throws IOException {
                        return read();
                    }
                    @Override public int read(byte[]b, int off, int len) throws IOException {
                        return read();
                    }                    
                };                       
                try (DigestBlobStore.IncomingBlob ib2 = t.bs.new IncomingBlob(badStream)) {
                    fail("IOException expected");
                } catch (IOException expected) {
                }
            }
        }    
    }
    
    // -- BlobReference --------------------------------------------------------
    
    private void expectBlobReferenceFailure(DigestBlobStore db, String id) {
        try {
            DigestBlobStore.BlobReference r = db.new BlobReference(id);
            fail("Blob reference for [" + id + "] should have failed, but did not.");
        } catch (Exception expected) {}
    }
    
    private void expectBlobReferenceFailure(DigestBlobStore db, byte[] digest) {
        try {
            DigestBlobStore.BlobReference r = db.new BlobReference(digest);
            fail("Blob reference for [" + Objects.toString(digest) + "] should have failed, but did not.");
        } catch (Exception expected) {}
    }

    @Test
    public void testBlobReference() throws IOException {
        try (TestBlobStoreMd5 t = new TestBlobStoreMd5(Configuration.unix())) {
            // string-based constructors
            DigestBlobStore.BlobReference r = t.bs.new BlobReference("e19c1283c925b3206685ff522acfe3e6");    // echo "this is a test" | md5sum            
            assertEquals("e19c1283c925b3206685ff522acfe3e6", r.id);                                        // round-trip to string
            assertEquals("e19c1283c925b3206685ff522acfe3e6", t.bs.new BlobReference(r.id.toUpperCase()).id); // round-trip from uppercase to string
            
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3e60");  // too long (odd) - appended 0
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3e600"); // too long (even) - appended 00
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3e");    // too short (odd) - dropped 6
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3");     // too short (even) - appended e6
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3eS");   // replaced last char with non-hex S
            expectBlobReferenceFailure(t.bs, " e19c1283c925b3206685ff522acfe3e6");  // leading space
            expectBlobReferenceFailure(t.bs, "e19c1283c925b3206685ff522acfe3e6 ");  // trailing space
            expectBlobReferenceFailure(t.bs, (String) null);                        // null
            expectBlobReferenceFailure(t.bs, "");                                   // empty string
            
            // byte[]-based constructors
            r = t.bs.new BlobReference(IO.parseHex("e19c1283c925b3206685ff522acfe3e6"));  // valid
            assertEquals("e19c1283c925b3206685ff522acfe3e6", r.id);                       // round-trip to string
            expectBlobReferenceFailure(t.bs, new byte[] {0x00});         // too short
            expectBlobReferenceFailure(t.bs, (byte[]) null);             // null digest
            expectBlobReferenceFailure(t.bs, new byte[0]);               // empty digest
        }
    }
    
    class TestBlobStoreMd5 implements AutoCloseable {

        private FileSystem fs;
        private Path path;
        private String configname;
        private DigestBlobStore bs;
        
        TestBlobStoreMd5(Configuration cfg) throws IOException {
            init(cfg, DigestBlobStore.DEFAULT_MAXBLOBS_PER_DIRECTORY, callingMethod());
        }
        
        TestBlobStoreMd5(Configuration cfg, int blobsPerDir) throws IOException {
            init(cfg, blobsPerDir, callingMethod());
        }
        
        private String callingMethod() {
            return Thread.currentThread().getStackTrace()[3].getMethodName();
        }
        
        private void init(Configuration cfg, int blobsPerDir, String callingMethodName) throws IOException {
            StringBuilder p = new StringBuilder();
            
            char sep = '/';                        
            if (cfg.equals(Configuration.windows())){
                configname = "windows";
                sep = '\\';
                p.append("C:");
            } else if (cfg.equals(Configuration.unix())) {
                configname = "unix";
            } else if (cfg.equals(Configuration.osX())) {
                configname = "osX";
            } else {
                configname = "UNKNOWN";
            }
            p.append(sep);
            p.append(configname);
            p.append(sep);
            p.append(callingMethodName);
            
            fs = Jimfs.newFileSystem(cfg);
            
            path = fs.getPath(p.toString());
            LOG.debug("Creating TestBlobStore at {}", path);
            bs = new DigestBlobStore(path, "MD5", blobsPerDir);
        }
                
        @Override
        public void close() throws IOException {
            LOG.debug("Destroying TestBlobStore at {}", path);
            bs.close();
            fs.close();
        }
    }
}
