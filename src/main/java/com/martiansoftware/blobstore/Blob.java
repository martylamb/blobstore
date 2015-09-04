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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A Blob of arbitrary binary data that may or may not exist within a
 * BlobStore, with methods for reading the data and deleting the Blob.
 * 
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class Blob {
    
    private final BlobStore _bs;
    private final Ref _ref;
    private final Path _path;
    
    Blob(BlobStore bs, Ref ref) {
        _bs = bs;
        _ref = ref;
        _path = bs._data.resolve(_ref.toPath());
    }
   
    /** Returns the BlobStore containing this Blob.
        @return the BlobStore containing this Blob. */
    public BlobStore blobStore() { return _bs; }
    
    /** Returns this Blob's Ref
        @return this Blob's Ref */
    public Ref ref() { return _ref; }
    
    /** Returns true iff this Blob actually exists within the BlobStore.
        @return true iff this Blob actually exists within the BlobStore. */
    public boolean exists() { return Files.exists(_path); }
    
    /** Provides an InputStream for reading this Blob's data.
        @return an InputStream for reading this Blob's data.
        @throws IOException */
    public InputStream inputStream() throws IOException { return Files.newInputStream(_path); }
    
    /** Reads this Blob in its entirety into a new byte array.
        @return a new byte array containing this Blob's data
        @throws IOException */
    public byte[] readAllBytes() throws IOException { return Files.readAllBytes(_path); }
    
    /** Returns the size of this Blob (number of bytes)
        @return the size of this Blob (number of bytes)
        @throws IOException */
    public long size() throws IOException { return Files.size(_path); }
    
    /** Copies this Blob to the specified Path.
        @param dest the Path to the destination File
        @param options options specifying how the copy should be done
        @return the Path to the destination File
        @throws IOException */
    public Path copyTo(Path dest, CopyOption... options) throws IOException { return Files.copy(_path, dest, options); }
    
    /** Deletes this Blob, removing it from the BlobStore.
        @return this Blob
        @throws IOException */
    public Blob delete() throws IOException { _bs.delete(_ref); return this; }
    
    @Override public String toString() {
        return String.format("Blob %s in BlobStore %s", _ref, _bs.location().toAbsolutePath());
    }

    @Override public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this._bs);
        hash = 97 * hash + Objects.hashCode(this._ref);
        hash = 97 * hash + Objects.hashCode(this._path);
        return hash;
    }

    @Override public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        final Blob other = (Blob) obj;
        if (!Objects.equals(this._bs, other._bs)) return false;
        if (!Objects.equals(this._ref, other._ref)) return false;
        return (Objects.equals(this._path, other._path));
    }
}
