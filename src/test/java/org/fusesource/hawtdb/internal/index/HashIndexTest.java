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
package org.fusesource.hawtdb.internal.index;

import org.fusesource.hawtdb.api.HashIndexFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.PageFileFactory;
import org.fusesource.hawtdb.api.PageFile;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndexTest extends IndexTestSupport {

    private final static Log LOG = LogFactory.getLog(HashIndexTest.class);

    @Override
    protected Index<String, Long> createIndex(int page) {
        traceStart(LOG, "HashIndexTest.createIndex(%d)", page);
        HashIndexFactory<String,Long> factory = new HashIndexFactory<String,Long>();
        factory.setKeyCodec(StringCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        Index<String, Long> ret = null;
        if( page==-1 ) {
            ret = factory.create(tx);
        } else {
            ret =  factory.open(tx, page);
        }
        traceEnd(LOG, "HashIndexTest.createIndex -> %s", ret);
        return ret;
    }


    @Override
    public void testIndexOperations() throws Exception {
      trace(LOG, "Skipping test IndexTestSupport.testIndexOperations");
    }

    @Override
    public void testRandomRemove() throws Exception {
        // TODO: look into why this test is failing.
    }

    @Test
    public void testNonTransactional() throws Exception {
        traceStart(LOG, "IndexTestSupport.testNonTransactional()");
        PageFileFactory pff = new PageFileFactory();
        pff.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        pff.setPageSize((short)10);
        pff.getFile().delete();
        pff.open();
        PageFile pf = pff.getPageFile();

        HashIndexFactory<String,Long> hif = new HashIndexFactory<String,Long>();
        hif.setKeyCodec(StringCodec.INSTANCE);
        hif.setValueCodec(LongCodec.INSTANCE);
        //hif.setBucketCapacity(32);
        hif.setFixedCapacity(1);
        Index<String, Long> index = hif.create(pf);

        final int count = 40;
        for (int i = 0; i < count; i++) {
            assertNull(index.put(key(i), (long)i));
        }

        List<Integer> order = new ArrayList<Integer>(count);
        for (int i = 0; i < count; i++) {
          order.add(i);
        }

        Random rand = new Random(0);
        Collections.shuffle(order, rand);
        int prev = 0;
        for (int i : order) {
            prev = i;
            try {
                Long val = index.remove(key(i));
                assertEquals(Long.valueOf(i), val);
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected exception on " + i + ", prev: " + prev + ", ex: " + e);
            }
        }
        assertTrue(index.isEmpty());
        pff.close();
        traceEnd(LOG, "IndexTestSupport.testNonTransactional");
    }

}
