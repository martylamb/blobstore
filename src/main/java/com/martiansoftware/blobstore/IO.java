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
import com.martiansoftware.hex.StandardHexCodecs;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of filesystem and io utility methods, plus a wrapper around
 * java.nio.Files that adds metrics
 * 
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
class IO {

    private static final Logger LOG = LoggerFactory.getLogger(IO.class);

    private final MetricRegistry _metrics;
    private final Counter existsCounter, 
                            createDirectoriesCounter,
                            isDirectoryCounter,
                            listCounter,
                            deleteIfExistsCounter,
                            newDirectoryStreamCounter,
                            moveAtomicCounter,
                            newOutputStreamCounter,
                            sizeCounter;

    /**
     * Creates a new IO interface that tracks usage in the supplied MetricRegistry
     * @param metrics the MetricRegistry for IO usage tracking
     */
    IO(MetricRegistry metrics) {
        _metrics = metrics;
        existsCounter = _metrics.register("Files.exists", new Counter());
        createDirectoriesCounter = _metrics.register("Files.createDirectories", new Counter());
        isDirectoryCounter = _metrics.register("Files.isDirectory", new Counter());
        listCounter = _metrics.register("Files.list", new Counter());
        deleteIfExistsCounter = _metrics.register("Files.deleteIfExists", new Counter());
        newDirectoryStreamCounter = _metrics.register("Files.newDirectoryStream", new Counter());
        moveAtomicCounter = _metrics.register("Files.move(StandardCopyOption.ATOMIC_MOVE)", new Counter());
        newOutputStreamCounter = _metrics.register("Files.newOutputStream", new Counter());
        sizeCounter = _metrics.register("Files.size", new Counter());
    }

    /**
     * @see java.nio.Files.exists(java.nio.Path);
     */
    boolean exists(Path path) {
        existsCounter.inc();
        return Files.exists(path);
    }
    
    /**
     * @see java.nio.Files.createDirectories(java.nio.Path);
     */
    Path createDirectories(Path dir) throws IOException {
        createDirectoriesCounter.inc();
        return Files.createDirectories(dir);
    }
    
    /**
     * @see java.nio.Files.isDirectory(java.nio.Path);
     */
    boolean isDirectory(Path path) throws IOException {
        isDirectoryCounter.inc();
        return Files.isDirectory(path);
    }
    
    /**
     * @see java.nio.Files.list(java.nio.Path);
     */
    Stream<Path> list(Path dir) throws IOException {
        listCounter.inc();
        return Files.list(dir);
    }
    
    /**
     * @see java.nio.Files.deleteIfExists(java.nio.Path);
     */
    public boolean deleteIfExists(Path path) throws IOException {
        deleteIfExistsCounter.inc();
        return Files.deleteIfExists(path);
    }
    
    /**
     * @see java.nio.Files.newDirectoryStream(java.nio.Path);
     */
    public DirectoryStream<Path> newDirectoryStream(Path dir) throws IOException {
        newDirectoryStreamCounter.inc();
        return Files.newDirectoryStream(dir);
    }
    
    /**
     * @see java.nio.Files.newOutputStream(java.nio.Path);
     */
    public OutputStream newOutputStream(Path path) throws IOException {
        newOutputStreamCounter.inc();
        return Files.newOutputStream(path);        
    }
    
    /**
     * @see java.nio.Files.size(java.nio.Path);
     */
    public long size(Path path) throws IOException {
        sizeCounter.inc();
        return Files.size(path);
    }
    
//  additional NON-java.io.Files wrapper IO utilities are below ----------------
    
    static byte[] parseHex(String s) {
        try {
            return StandardHexCodecs.STRICT.decode(s);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
    
    Path ensureDirectoryExists(Path dir) throws IOException {
        if (!exists(dir)) {
            createDirectories(dir);
        }
        if (!isDirectory(dir)) {
            throw new IOException(String.format("%s exists but is not a directory", dir));
        }
        return dir;
    }

    long copyAndClose(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[32 * 1024];
        int bytesRead;
        long bytesWritten = 0;
        try {
            while ((bytesRead = in.read(buf)) > 0) {
                out.write(buf, 0, bytesRead);
                bytesWritten += bytesRead;
            }
        } finally {
            out.close();
            in.close();
        }
        return bytesWritten;
    }

    // returns true iff it deleted the dir
    boolean deleteDirIfEmpty(Path dir) throws IOException {
        if (isDirectory(dir) && !list(dir).findFirst().isPresent()) {
            deleteIfExists(dir);
            return true;
        }
        return false;
    }

    Path moveAtomic(Path from, Path to) throws IOException {
        moveAtomicCounter.inc();
        Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        return to;
    }

    private void delete(Path p) throws IOException {
        boolean REALLY_DELETE = true; // sometimes set to false during development/debugging
        if (REALLY_DELETE) {
            deleteIfExists(p);
        } else {
            LOG.info("simulating deletion of {}", p);
        }
    }
    
    // recursively delete the contents of the specified directory, optionally including removing
    // the specified directory itself.
    void recursiveDeleteDirectory(Path dir, boolean deleteTop) throws IOException {
        Path top = dir;
        if (!exists(dir)) return;
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e == null) {
                    if (deleteTop || !dir.equals(top)) {
                        delete(dir);
                    }
                    return FileVisitResult.CONTINUE;
                } else {
                    throw e;
                }
            }
        });
    }
}
