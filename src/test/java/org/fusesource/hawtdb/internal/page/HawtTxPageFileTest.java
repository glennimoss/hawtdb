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
import org.fusesource.hawtdb.internal.io.MemoryMappedFile;
import org.fusesource.hawtdb.internal.page.HawtPageFile;
import org.fusesource.hawtdb.internal.page.HawtTxPageFile;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;


/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HawtTxPageFileTest {

    private final static Log LOG = LogFactory.getLog(HawtTxPageFileTest.class);

    @Test
    public void testTrivial() throws Exception {
        traceStart(LOG, "HawtTxPageFileTest.testTrivial()");

        File file = new File("target/test-data/" + getClass().getName() + ".db");
        file.delete();
        TxPageFileFactory pff = new TxPageFileFactory();
        pff.setFile(file);
        pff.setMappingSegementSize(5);
        pff.setPageSize((short)9);
        pff.open();
        HawtTxPageFile pf = (HawtTxPageFile)pff.getTxPageFile();

        pff.close();

        pff.open();
        traceEnd(LOG, "HawtTxPageFileTest.testTrivial");
    }

    @Test
    public void testCorruptedHeader() throws Exception {
        traceStart(LOG, "HawtTxPageFileTest.testCorruptedHeader()");
        File file = new File("target/test-data/" + getClass().getName() + "-corrupt.db");
        file.delete();
        TxPageFileFactory pff = new TxPageFileFactory();
        pff.setFile(file);
        pff.setMappingSegementSize(5);
        pff.setPageSize((short)9);
        pff.open();
        HawtTxPageFile pf = (HawtTxPageFile)pff.getTxPageFile();

        pff.close();

        long bogus = 0xABCDEF012345789AL;

        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        //raf.seek(HawtTxPageFile.FILE_HEADER_SIZE/2-8);
        raf.writeLong(bogus);
        raf.close();

        pff.open();
        traceEnd(LOG, "HawtTxPageFileTest.testCorruptedHeader");
    }


}
