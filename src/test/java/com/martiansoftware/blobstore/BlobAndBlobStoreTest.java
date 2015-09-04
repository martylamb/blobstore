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

import com.martiansoftware.blobstore.Ref;
import com.martiansoftware.blobstore.Blob;
import com.martiansoftware.blobstore.BlobStore;
import com.martiansoftware.blobstore.FSUtils;
import com.martiansoftware.blobstore.BlobStore.RuntimeNoSuchAlgorithmException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class BlobAndBlobStoreTest {
    
    private Path _blobDir;
    
    public BlobAndBlobStoreTest() {
    }
    
    @Before public void setUp() {
        try {
            _blobDir = Files.createTempDirectory(BlobAndBlobStoreTest.class.getCanonicalName() + "-");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @After public void tearDown() {
        try {
            FSUtils.recursiveDeleteDirectory(_blobDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test public void testConstructors() throws IOException {
        BlobStore bs = new BlobStore(_blobDir.resolve("bs1"));        
        assertEquals(_blobDir.resolve("bs1"), bs.location());
        
        BlobStore bs2 = new BlobStore(_blobDir.resolve("bs1"));
        assertEquals("SHA-1", bs2.digestName());
        
        BlobStore bs3 = new BlobStore(_blobDir.resolve("bs3"), new BlobStore.NamedDigestSupplier("MD5"));
        assertEquals("MD5", bs3.digestName());
        
        try {
            BlobStore bs4 = new BlobStore(_blobDir.resolve("bs4"), "NONEXISTENT-ALGORITHM");
            fail("Expected RuntimeNoSuchAlgorithmException");
        } catch (RuntimeNoSuchAlgorithmException expected) {}
                
        Path p = _blobDir.resolve("bs5");
        Files.createFile(p); // put a file where we are going to try to create a blob directory
        try {
            BlobStore bs5 = new BlobStore(_blobDir.resolve(p));
            fail("Expected IOException");
        } catch (IOException expected) {}
        
        try {
            BlobStore bs6 = new BlobStore(_blobDir.resolve("bs6"), 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {}
        
        BlobStore bs7 = new BlobStore(_blobDir.resolve("bs7"), 16);
    }
    
    @Test public void testToString() throws IOException {
        Path p = _blobDir.resolve("bs");
        BlobStore bs = new BlobStore(p);
        assertEquals("BlobStore " + p.toAbsolutePath() + " using digest SHA-1 with digest length 20", bs.toString());
    }

    @Test public void testAddBytes() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testAddBytes");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.add(b);
        assertEquals("8b6ccb43dca2040c3cfbcd7bfff0b387d4538c33", blob.ref().toString());
        Blob blob2 = bs.get(new Ref("8b6ccb43dca2040c3cfbcd7bfff0b387d4538c33"));
        assertEquals(blob, blob2);
    }

    @Test public void testUnpermittedAddBytes() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testAddBytes");
        BlobStore bs = new BlobStore(p, 10);
        try {
            bs.add(b);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {}
    }
    
    private Path createTestFile(Path p) throws IOException {
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(p))) {
            for (int i = 0; i < 1024 * 1024 * 10; ++i) { // 10MB file
                out.write(i & 0xff);
            }
        }
        return p;
    }
    
    @Test public void testAddInputStream() throws IOException {
        InputStream in = new BufferedInputStream(Files.newInputStream(createTestFile(_blobDir.resolve("testAddInputStream"))));
        Path p = _blobDir.resolve("bs-testAddBytes");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.add(in);
        assertEquals("d6bca95f69190776042815017281649a9c9fd2a9", blob.ref().toString());
        Blob blob2 = bs.get(new Ref("d6bca95f69190776042815017281649a9c9fd2a9"));
        assertEquals(blob, blob2);
    }

    @Test public void testAddFile() throws IOException {
        File f = createTestFile(_blobDir.resolve("testAddFile")).toFile();
        Path p = _blobDir.resolve("bs-testAddFile");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.add(f);
        assertEquals("d6bca95f69190776042815017281649a9c9fd2a9", blob.ref().toString());
        Blob blob2 = bs.get(new Ref("d6bca95f69190776042815017281649a9c9fd2a9"));
        assertEquals(blob, blob2);  
        assertEquals(blob, bs.add(f)); // duplicate add
    }


    @Test public void testBadRefs() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testPutBytes");
        BlobStore bs = new BlobStore(p);
        
        try {
            Blob blob = bs.put(new Ref("000000"), b);
            fail("Expected IOException for short ref!");
        } catch (IllegalArgumentException expected) {}
        
        try {
            Blob blob = bs.put(new Ref("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"), b);
            fail("Expected IOException for long ref!");
        } catch (IllegalArgumentException expected) {}
    }
    
    @Test public void testPutBytes() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testPutBytes");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.put(new Ref("0000000000000000000000000000000000000000"), b);
        assertEquals("0000000000000000000000000000000000000000", blob.ref().toString());
        Blob blob2 = bs.get(new Ref("0000000000000000000000000000000000000000"));
        assertEquals(blob, blob2);        
    }
    
    @Test public void testPutFile() throws IOException {
        File f = createTestFile(_blobDir.resolve("testPutFile")).toFile();
        Path p = _blobDir.resolve("bs-testPutFile");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.put(new Ref("1111111111111111111111111111111111111111"), f);
        assertEquals("1111111111111111111111111111111111111111", blob.ref().toString());
        Blob blob2 = bs.get(new Ref("1111111111111111111111111111111111111111"));
        assertEquals(blob, blob2);                
    }
    
    @Test public void delete() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testDelete");
        BlobStore bs = new BlobStore(p);
        Ref ref = new Ref("dddddddddddddddddddddddddddddddddddddddd");
        Blob blob = bs.put(ref, b);
        assertEquals(ref, blob.ref());
        Blob blob2 = bs.get(ref);
        assertTrue(blob2.exists());
        
        Blob blob3 = bs.delete(ref);
        assertFalse(blob3.exists());
        assertEquals(blob2, blob3);
        assertFalse(blob2.exists());
        
        Blob blob4 = blob3.delete(); // double-delete is idempotent
        assertFalse(blob4.exists());
        assertEquals(blob2, blob4);
        
        // now delete two objects from same intermediate dir
        Ref r0 = new Ref("0000000000000000000000000000000000000000");
        Ref r1 = new Ref("0000000000000000000000000000000000000001");
        bs.put(r0, b);
        bs.put(r1, b);
        bs.delete(r0);
        bs.delete(r1);
        assertFalse(bs.get(r1).exists());
        
        // now delete something that's not present, including intermediate dir
        bs.delete(new Ref("9900000000000000000000000000000000000001"));
    }
    
    @Test public void testStream() throws IOException {
        byte[] b = "This is a test!".getBytes(StandardCharsets.UTF_8);
        Path p = _blobDir.resolve("bs-testStream");
        BlobStore bs = new BlobStore(p);
        
        Set<String> t1 = new java.util.HashSet<>();
        for (int i = 0; i < 1024; ++i) {
            String s = "This is test number " + i;
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            t1.add(s);
            bs.add(bytes);
        }
        
        Set<String> t2 = new java.util.HashSet<>();
        bs.stream().forEach(blob -> {
            try {
                t2.add(new String(blob.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        
        assertEquals(t1, t2);
    }

    @Test public void testBlobStuff() throws IOException {
        byte[] b = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Path p = _blobDir.resolve("bs-testBlobStuff");
        BlobStore bs = new BlobStore(p);
        Blob blob = bs.add(b);
        
        assertEquals(bs, blob.blobStore());
        assertEquals(10, blob.size());
        
        try (InputStream in = blob.inputStream()) {
            for (int i = 0; i < 10; ++i) assertEquals(i, in.read());
            assertEquals(-1, in.read());
        }
        
        Path p2 = _blobDir.resolve("testBlobStuff-testfile");
        blob.copyTo(p2);
        
        Blob blob2 = bs.add(p2.toFile());
        assertEquals(blob2, blob);
        assertEquals(10, blob2.size());
        
        assertFalse(blob.equals(null));
        assertFalse(blob.equals("Ceci n'est pas un blob."));
        assertEquals(blob.hashCode(), blob2.hashCode());
        assertFalse(blob == blob2);
        assertEquals(blob.toString(), "Blob 494179714a6cd627239dfededf2de9ef994caf03 in BlobStore " + bs.location().toAbsolutePath());

        BlobStore bs2 = new BlobStore(_blobDir.resolve("bs-testBlobStuff2"));
        Ref ref = new Ref("494179714a6cd627239dfededf2de9ef994caf03");
        assertFalse(bs.get(ref).equals(bs2.get(ref)));
        Ref ref2 = new Ref("494179714a6cd627239dfededf2de9ef994caf04");
        assertFalse(bs.get(ref).equals(bs.get(ref2)));
    }
}
