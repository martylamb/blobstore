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
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
class FSUtils {

    
    static void deleteDirIfEmpty(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            try {
                if(!Files.list(dir).findFirst().isPresent()) Files.deleteIfExists(dir);
            } catch (DirectoryNotEmptyException|NoSuchFileException ignored) {}
        }
    }
    
    private static final boolean REALLY_DELETE = true;
    private static final boolean LOG_DELETES = false;
    static void recursiveDeleteDirectory(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (LOG_DELETES) System.out.println("Deleting FILE " + file);
                if (REALLY_DELETE) Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e == null) {
                    if (LOG_DELETES) System.out.println("Deleting DIR " + dir);
                    if (REALLY_DELETE) Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                } else throw e;
            }
        });        
    }
}
