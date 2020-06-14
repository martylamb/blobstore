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
package com.martiansoftware.blobstore.example;

import com.martiansoftware.blobstore.Blob;
import com.martiansoftware.blobstore.BlobStore;
import com.martiansoftware.blobstore.DigestBlobStore;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Example code for README.md
 * 
 * @author <a href="http://martylamb.com">Marty Lamb</a>
 */
public class Example {

    public static void main(String[] args) throws Exception {
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
    }
}
