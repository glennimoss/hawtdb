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
package org.fusesource.hawtdb.internal.page;

import java.io.File;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtdb.internal.io.MemoryMappedFile;
import org.fusesource.hawtdb.internal.page.HawtPageFile;
import org.fusesource.hawtdb.internal.page.HawtTxPageFile;
import org.fusesource.hawtdb.internal.page.ExtentOutputStream;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 *
 * @author Glenn Moss <glennimoss+hawtdb@gmail.com>
 */
public class ExtentOutputStreamTest {

    private final static Log LOG = LogFactory.getLog(ExtentOutputStreamTest.class);

    @Test
    public void testPathological() throws Exception {
        traceStart(LOG, "ExtentOutputStreamTest.testPathological()");

        File file = new File("target/test-data/" + getClass().getName() + ".db");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 9);

        HawtPageFile hpf = new HawtPageFile(mmf, (short)9, 0, 100);


        try {
          ExtentOutputStream eos = new ExtentOutputStream(hpf, (short)1);

          Assert.fail("Should not allow pathological case");
        } catch (IllegalArgumentException e) {
          // Expected not to allow the pathological case
        }

        traceEnd(LOG, "ExtentOutputStreamTest.testPathological");
    }

    @Test
    public void testEOS() throws Exception {
        traceStart(LOG, "ExtentOutputStreamTest.testEOS()");

        File file = new File("target/test-data/" + getClass().getName() + ".db");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 9);

        HawtPageFile hpf = new HawtPageFile(mmf, (short)9, 0, 100);

        ExtentOutputStream eos = new ExtentOutputStream(hpf, (short)2);

        byte[] data = "hello world".getBytes("UTF-8");

        eos.write(data, 0, data.length);

        eos.close();
        mmf.close();

        Ranges pages = eos.getPages();
        Assert.assertEquals(4, pages.size());
        Assert.assertTrue(pages.contains(0));
        Assert.assertTrue(pages.contains(1));
        Assert.assertTrue(pages.contains(2));
        Assert.assertTrue(pages.contains(3));


        DataByteArrayOutputStream out = new DataByteArrayOutputStream(36);
        out.writeBytes("x");   // [0]     first extent   page 0
        out.writeInt(18);      // [1,4]
        out.writeInt(2);       // [5,8]
        out.write(data, 0, 9); // [9,17]                 page 1
        out.writeBytes("x");   // [18]     next extent   page 2
        out.writeInt(11);      // [19,22]
        out.writeInt(-1);      // [23,26]
        out.write(data, 9, 2); // [27,28]                page 3
        // 0x00 times 7        // [29,35]

        byte[] expected = out.getData();
        byte[] found = new byte[expected.length];

        RandomAccessFile raf = new RandomAccessFile(file, "r");
        Assert.assertEquals(expected.length, raf.length());
        raf.readFully(found);
        raf.close();

        Assert.assertArrayEquals(expected, found);

        traceEnd(LOG, "ExtentOutputStreamTest.testEOS");
    }
}
