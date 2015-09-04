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
import java.nio.file.Paths;
import javax.xml.bind.DatatypeConverter;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class RefTest {
    
    public RefTest() {
    }
    
    // utility to create a new byte array of specified length
    private byte[] newBytes(int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; ++i) b[i] = (byte) (i & 0xff);
        return b;
    }
    
    @Test public void testConstruction() {
        Ref r;

        try { r = new Ref((byte[]) null); fail("Expected exception for null byte array"); } catch (NullPointerException expected) {}
        try { r = new Ref((String) null); fail("Expected exception for null String"); } catch (NullPointerException expected) {}
        
        try { r = new Ref(newBytes(0)); fail("Expected exception for 0-byte array"); } catch (IllegalArgumentException expected) {}
        try { r = new Ref(newBytes(1)); fail("Expected exception for 1-byte array"); } catch (IllegalArgumentException expected) {}
        
        try { r = new Ref(""); fail("Expected exception for empty String"); } catch (IllegalArgumentException expected) {}
        try { r = new Ref("00"); fail("Expected exception for 1-byte String"); } catch (IllegalArgumentException expected) {}
        try { r = new Ref("z2qq"); fail("Expected exception for non-hex String"); } catch (IllegalArgumentException expected) {}
        try { r = new Ref("00112"); fail("Expected exception for odd-length hex String"); } catch (IllegalArgumentException expected) {}

        for (int i = 2; i < 1024; ++i) {
            r = new Ref(newBytes(i));
            r = new Ref(DatatypeConverter.printHexBinary(newBytes(i)));
            r = new Ref(Paths.get("99", DatatypeConverter.printHexBinary(newBytes(i))));
        }
        
        r = new Ref(Paths.get("01/2345"));
        try { r = new Ref(Paths.get("1234/56")); fail("Expected exception for invalid parent Path"); } catch (IllegalArgumentException expected) {}
        try { r = new Ref(Paths.get("zz/1234")); fail("Expected exception for invalid parent Path"); } catch (IllegalArgumentException expected) {}
    }

    @Test public void testLength() {
        Ref r = new Ref(newBytes(3));
        assertEquals(3, r.length());
    }

    @Test public void testToPath() {
        Ref r = new Ref("0123456789");
        assertEquals(Paths.get("01/23456789"), r.toPath());
    }

    @Test public void testToString() {
        Ref r = new Ref("0123456789abcdef");
        assertEquals("0123456789abcdef", r.toString());
        
        r = new Ref("0123456789ABCDEF");
        assertEquals("0123456789abcdef", r.toString());
    }

    private void assertNotEquals(Object o1, Object o2) {
        assertFalse(o1.equals(o2));    
    }
    
    @Test public void testHashCode() {
        Ref r1 = new Ref("00112233");
        Ref r2 = new Ref("00112233");
        Ref r3 = new Ref("001122");
        Ref r4 = new Ref("11223344");
        
        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r1.hashCode(), r3.hashCode());
        assertNotEquals(r2.hashCode(), r4.hashCode());
    }

    @Test public void testEquals() {
        Ref r1 = new Ref("00112233");
        Ref r2 = new Ref("00112233");
        Ref r3 = new Ref("001122");
        Ref r4 = new Ref("11223344");
        
        assertEquals(r1, r2);
        assertNotEquals(r1, null);
        assertNotEquals(r1, "hello");

        assertEquals(r2, r1);
        assertNotEquals(r1, r3);
        assertNotEquals(r2, r4);        
    }
    
}
