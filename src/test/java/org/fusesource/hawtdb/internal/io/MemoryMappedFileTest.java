/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.internal.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.fusesource.hawtdb.internal.io.MemoryMappedFile;
import org.fusesource.hawtbuf.Buffer;

import org.junit.Assert;


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MemoryMappedFileTest {

    @org.junit.Test
    public void basicOps() throws IOException {
        File file = new File("target/test-data/" + getClass() + "/basicOps.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 1024*1024*100);

        int PAGE_SIZE = 1024*4;
        int LAST_PAGE = 100;

        byte expect[] = createData(PAGE_SIZE);

        mmf.write(0, expect);
        mmf.write(LAST_PAGE *PAGE_SIZE, expect);

        // Validate data on the first page.
        byte actual[] = new byte[PAGE_SIZE];
        mmf.read(0, actual);
        Assert.assertEquals('a', actual[0]);
        Assert.assertEquals('a', actual[26]);
        Assert.assertEquals('z', actual[26+25]);

        // Validate data on the 3rd page.
        actual = new byte[PAGE_SIZE];
        mmf.read(PAGE_SIZE*LAST_PAGE, actual);
        Assert.assertEquals('a', actual[0]);
        Assert.assertEquals('a', actual[26]);
        Assert.assertEquals('z', actual[26+25]);

        mmf.close();

    }

    private byte[] createData(int size) {
        byte[] rc = new byte[size];
        for (int i = 0; i < rc.length; i++) {
          rc[i] = (byte) ('a'+(i%26));
        }
        return rc;
    }

    @org.junit.Test
    public void unalignedWrite() throws IOException {
        File file = new File("target/test_data/" + getClass() + "/unalignedWrite.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 5);


        byte[] expect = "\0\0\0hello world".getBytes("UTF-8");

        mmf.write(3, expect, 3, 11);

        mmf.close();


        RandomAccessFile raf = new RandomAccessFile(file, "r");
        byte[] found = new byte[14];
        raf.read(found, 0, found.length);

        Assert.assertArrayEquals(expect, found);
    }

    @org.junit.Test
    public void unalignedWriteByteBuffer() throws IOException {
        File file = new File("target/test-data/" + getClass() + "/unalignedWriteByteBuffer.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 5);


        byte[] expect = "\0\0\0hello world".getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.wrap(expect);
        buf.position(3);

        mmf.write(3, buf);

        mmf.close();


        RandomAccessFile raf = new RandomAccessFile(file, "r");
        byte[] found = new byte[14];
        raf.read(found, 0, found.length);

        Assert.assertArrayEquals(expect, found);
    }


    @org.junit.Test
    public void unalignedWriteAndUnalignedRead() throws IOException {
        File file = new File("target/test_data/" + getClass() + "/unalignedWriteAndUnalignedRead.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 5);

        byte[] expect = "hello world".getBytes("UTF-8");

        mmf.write(3, expect, 0, 11);

        byte[] found = new byte[11];
        mmf.read(3, found, 0, found.length);

        Assert.assertArrayEquals(expect, found);

        mmf.close();
    }

    @org.junit.Test
    public void unalignedWriteAndAlignedRead() throws IOException {
        File file = new File("target/test_data/" + getClass() + "/unalignedWriteAndAlignedRead.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 5);

        byte[] expect = "\0\0\0hello world".getBytes("UTF-8");

        mmf.write(3, expect, 3, 11);

        byte[] found = new byte[14];
        mmf.read(0, found, 0, found.length);

        Assert.assertArrayEquals(expect, found);

        mmf.close();
    }

}
