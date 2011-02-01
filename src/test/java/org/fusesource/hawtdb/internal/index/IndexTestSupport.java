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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.fusesource.hawtdb.api.TxPageFile;
import org.fusesource.hawtdb.api.TxPageFileFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.Transaction;
import org.junit.After;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * Tests an Index
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class IndexTestSupport {

    private final static Log LOG = LogFactory.getLog(IndexTestSupport.class);

    private TxPageFileFactory pff;
    protected TxPageFile pf;
    protected Index<String,Long> index;
    protected Transaction tx;


    protected TxPageFileFactory createConcurrentPageFileFactory() {
        traceStart(LOG, "IndexTestSupport.createConcurrentPageFileFactory()");
        TxPageFileFactory rc = new TxPageFileFactory();
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        traceEnd(LOG, "IndexTestSupport.createConcurrentPageFileFactory -> %s", rc);
        return rc;
    }

    @After
    public void tearDown() throws Exception {
        traceStart(LOG, "IndexTestSupport.tearDown()");
        if( pf!=null ) {
            pff.close();
            pff = null;
        }
        traceEnd(LOG, "IndexTestSupport.tearDown");
    }

    abstract protected Index<String,Long> createIndex(int page);

    private static final int COUNT = 10000;

    public void createPageFileAndIndex(short pageSize) throws Exception {
        traceStart(LOG, "IndexTestSupport.createPageFileAndIndex(%d)", pageSize);
        pff = createConcurrentPageFileFactory();
        pff.setPageSize(pageSize);
        pff.getFile().delete();
        pff.open();
        pf = pff.getTxPageFile();
        tx = pf.tx();
        index = createIndex(-1);
        traceEnd(LOG, "IndexTestSupport.createPageFileAndIndex");
    }

    protected void reloadAll() {
        traceStart(LOG, "IndexTestSupport.reloadAll()");
        int page = index.getIndexLocation();
        pff.close();
        pff.open();
        pf = pff.getTxPageFile();
        tx = pf.tx();
        index = createIndex(page);
        traceEnd(LOG, "IndexTestSupport.reloadAll");
    }

    protected void reloadIndex() {
        traceStart(LOG, "IndexTestSupport.reloadIndex()");
        int page = index.getIndexLocation();
        tx.commit();
        index = createIndex(page);
        traceEnd(LOG, "IndexTestSupport.reloadIndex");
    }

    @Test
    public void testIndexOperations() throws Exception {
        traceStart(LOG, "IndexTestSupport.testIndexOperations()");
        createPageFileAndIndex((short) 500);
        reloadIndex();
        doInsert(COUNT);
        reloadIndex();
        checkRetrieve(COUNT);
        doRemove(COUNT);
        reloadIndex();
        doInsert(COUNT);
        doRemoveHalf(COUNT);
        doInsertHalf(COUNT);
        reloadIndex();
        checkRetrieve(COUNT);
        doPutIfAbsent();
        traceEnd(LOG, "IndexTestSupport.testIndexOperations");
    }

    @Test
    public void testRandomRemove() throws Exception {
        traceStart(LOG, "IndexTestSupport.testRandomRemove()");
        createPageFileAndIndex((short)100);
        final int count = 4000;
        doInsert(count);
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
                assertEquals((long) i, val);
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected exception on " + i + ", prev: " + prev + ", ex: " + e);
            }
            tx.commit();
        }
        assertTrue(index.isEmpty());

        traceEnd(LOG, "IndexTestSupport.testRandomRemove");
    }

    void doInsert(int count) throws Exception {
        traceStart(LOG, "IndexTestSupport.doInsert(%d)", count);
        for (int i = 0; i < count; i++) {
            index.put(key(i), (long)i);
        }
        tx.commit();
        traceEnd(LOG, "IndexTestSupport.doInsert");
    }

    protected String key(int i) {
        return "key:"+i;
    }

    void checkRetrieve(int count) throws IOException {
        traceStart(LOG, "IndexTestSupport.checkRetrieve(%d)", count);
        for (int i = 0; i < count; i++) {
            Long item = index.get(key(i));
            assertNotNull("Key missing: "+key(i), item);
        }
        traceEnd(LOG, "IndexTestSupport.checkRetrieve");
    }

    void doRemoveHalf(int count) throws Exception {
        traceStart(LOG, "IndexTestSupport.doRemoveHalf(%d)", count);
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertNotNull("Expected remove to return value for index "+i, index.remove(key(i)));
            }
        }
        tx.commit();
        traceEnd(LOG, "IndexTestSupport.doRemoveHalf");
    }

    void doInsertHalf(int count) throws Exception {
        traceStart(LOG, "IndexTestSupport.doInsertHalf(%d)", count);
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                index.put(key(i), (long)i);
            }
        }
        tx.commit();
        traceEnd(LOG, "IndexTestSupport.doInsertHalf");
    }

    void doRemove(int count) throws Exception {
        traceStart(LOG, "IndexTestSupport.doRemove(%d)", count);
        for (int i = 0; i < count; i++) {
            assertNotNull("Expected remove to return value for index "+i, index.remove(key(i)));
        }
        tx.commit();
        for (int i = 0; i < count; i++) {
            Long item = index.get(key(i));
            assertNull(item);
        }
        traceEnd(LOG, "IndexTestSupport.doRemove");
    }

    void doRemoveBackwards(int count) throws Exception {
        traceStart(LOG, "IndexTestSupport.doRemoveBackwards(%d)", count);
        for (int i = count - 1; i >= 0; i--) {
            index.remove(key(i));
        }
        tx.commit();
        for (int i = 0; i < count; i++) {
            Long item = index.get(key(i));
            assertNull(item);
        }
        traceEnd(LOG, "IndexTestSupport.doRemoveBackwards");
    }

    void doPutIfAbsent() throws Exception {
        traceStart(LOG, "IndexTestSupport.doPutIfAbsent()");
        index.put("myKey", 0L);
        // Do not put on existent key:
        assertEquals((Long) 0L, index.putIfAbsent("myKey", 1L));
        assertEquals((Long) 0L, index.get("myKey"));
        // Put on absent key:
        assertEquals(null, index.putIfAbsent("absent", 1L));
        assertEquals((Long) 1L, index.get("absent"));
        tx.commit();

        traceEnd(LOG, "IndexTestSupport.doPutIfAbsent");
    }
}
