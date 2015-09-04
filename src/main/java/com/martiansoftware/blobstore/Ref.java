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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;

/**
 * A reference to a Blob within a BlobStore.  This is essentially the key or
 * index into a given BlobStore for performing operations on a Blob.
 * 
 * In most cases this will be the hash of the Blob being stored, although that
 * is not a requirement (i.e., if using BlobStore.put() to add files to the
 * BlobStore).
 * 
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class Ref {

    // generally the hash of the blob (may not be if BlobStore.put() is used)
    private final byte[] _bytes;
    
    /**
     * Creates a new Ref using the specified byte array.
     * @param b generally the hash of the blob.  Must be at least two bytes long.
     */
    Ref(byte[] b) {
        _bytes = Arrays.copyOf(b, b.length);
        checkLength();
    }
    
    /**
     * Creates a new Ref using the specified hexadecimal String
     * @param s generally a String representation of the blob hash.  Must consist of an even
     * number of hexadecimal characters [0-9a-fA-F] and be at least four characters long.
     */
    Ref(String s) {
        checkHex(s);
        _bytes = DatatypeConverter.parseHexBinary(s);
        checkLength();
    }
    
    /**
     * Creates a new Ref using the specified Path.  The Path filename must consist of an
     * even number of hexadecimal characters.  The Path's parent's filename must consist of
     * exactly two hexadecimal characters.  The parent's filename and the Path's filename
     * are concatenated to form a hexadecimal String from which the Ref is built.
     * 
     * @param p the Path on which to base the new Ref
     */
    Ref(Path p) {
        StringBuilder sb = new StringBuilder();
        String s = p.getParent().getFileName().toString();
        checkHex(s);
        if (s.length() != 2) throw new IllegalArgumentException("Parent path must be 2-digit hexadecimal");
        sb.append(s);
        
        s = p.getFileName().toString();
        checkHex(s);
        sb.append(s);
        
        _bytes = DatatypeConverter.parseHexBinary(sb.toString());
    }
    
    /**
     * Returns the size of this Ref (NOT the Blob to which it points), in bytes.
     * If the Ref was generated from a MessageDigest (which happens automatically
     * in the BlobStore), then it is equivalent to the result of the digest's
     * getDigestLength() method.
     * @return the size of this Ref (NOT the Blob to which it points), in bytes
     */
    public int length() {
        return _bytes.length;
    }
    
    /**
     * Converts this Ref to a (relative) Path, with a Parent path consisting of
     * a lowercase hexadecimal representation of the Ref's first byte, and a child
     * Path consisting of a lowercase hexadecimal representation of the Ref's
     * remaining bytes.
     * 
     * @return a Path representation of this Ref
     */
    public Path toPath() {
        return Paths.get(toHex(_bytes, 0, 1), toHex(_bytes, 1, _bytes.length));
    }

    private void checkLength() {
        if (_bytes.length < 2) throw new IllegalArgumentException("Refs must be at least two bytes long.");
    }
    
    private void checkHex(String s) {
        if (!s.matches("^([0-9a-fA-F]{2})+$")) throw new IllegalArgumentException("Invalid hex string: '" + s + "'");
    }    
    
    private String toHex(byte[] b, int startInclusive, int endExclusive) {
        StringBuilder s = new StringBuilder();
        for (int i = startInclusive; i < endExclusive; ++i) s.append(String.format("%02x", b[i]));
        return s.toString();
    }
    
    @Override public String toString() {
        return DatatypeConverter.printHexBinary(_bytes).toLowerCase();
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 17 * hash + Arrays.hashCode(this._bytes);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Ref other = (Ref) obj;
        return Arrays.equals(this._bytes, other._bytes);
    }

}
