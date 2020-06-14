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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;

/**
 * A store of arbitrary binary objects ("Blobs").  A BlobStore ensures that
 * only one copy of any given Blob is stored.  Repeatedly adding the same
 * data to the BlobStore is idempotent.  Blobs are accessed via a (String)
 * identifier that uniquely identifies the Blob.  The format of this identifier
 * is dependent upon the BlobStore implementation.
 * 
 * @author <a href="http://martylamb.com">Marty Lamb</a>
 */
public interface BlobStore extends AutoCloseable {
    
    /**
     * Returns the number of Blobs in this BlobStore
     * @return the number of Blobs in this BlobStore
     */
    public long blobCount();
    
    /**
     * Returns the total number of bytes stored by this BlobStore.  This is
     * computed as the sum of the individual Blob sizes.
     * @return the total number of bytes stored by this BlobStore
     */
    public long byteCount();
    
    /**
     * Returns an Optional object containing Blob stored in this BlobStore with
     * the specified id, or else Optional.EMPTY if no such Blob exists in this
     * BlobStore
     * 
     * @param id the id of the Blob to retrieve
     * @return an Optional object containing the Blob stored in this BlobStore
     * with the specified id, or else Optional.EMPTY if no such Blob exists in
     * this BlobStore
     * @throws IOException if an error occurs while accessing the backing store
     */
    Optional<Blob> get(String id) throws IOException;
    
    /**
     * Adds the data at the specified Path to this BlobStore if it has not
     * previously been added.
     * 
     * @param sourcePath the Path to the file containing the Blob to add
     * @return the newly created Blob, or the previously added Blob if the same
     * data had been previously added
     * @throws IOException if an error occurs while accessing the backing store
     */
    Blob add(Path sourcePath) throws IOException;    

    /**
     * Adds the data at the specified InputStream to this BlobStore if it has
     * not previously been added.
     * 
     * @param sourceStream an InputStream containing the Blob to add
     * @return the newly created Blob, or the previously added Blob if the same
     * data had been previously added
     * @throws IOException if an error occurs while accessing the backing store
     */
    Blob add(InputStream sourceStream) throws IOException;    
    
    /**
     * Adds the data in the specified byte array to this BlobStore if it has not
     * previously been added.
     * 
     * @param sourceBytes a byte array containing the Blob to add
     * @return the newly created Blob, or the previously added Blob if the same
     * data had been previously added
     * @throws IOException if an error occurs while accessing the backing store
     */
    Blob add(byte[] sourceBytes) throws IOException;
    
    /**
     * Deletes the specified Blob from this BlobStore if it exists within
     * this BlobStore.
     * 
     * @param id the id of the Blob to delete
     * @return true if the Blob was deleted, or false if no such Blob exists
     * within this BlobStore
     * @throws IOException if an error occurs while accessing the backing store
     */
    boolean delete(String id) throws IOException;
    
    /**
     * Closes the BlobStore, allowing it to release any resources it might
     * be using.  After closing, the BlobStore may no longer be used.
     * 
     * @throws IOException if an error occurs while accessing the backing store
     */
    @Override void close() throws IOException;
}
