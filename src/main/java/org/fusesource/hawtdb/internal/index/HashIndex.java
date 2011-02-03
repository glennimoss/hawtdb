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

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.page.ExtentInputStream;
import org.fusesource.hawtdb.internal.page.ExtentOutputStream;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.fusesource.hawtdb.internal.index.Logging.debug;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * Hash Index implementation.  The hash buckets store entries in a b+tree.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {

    private final static Log LOG = LogFactory.getLog(HashIndex.class);

    private final BTreeIndexFactory<Key, Value> BIN_FACTORY = new BTreeIndexFactory<Key, Value>();

    private final Paged paged;
    private final int page;
    private final int maximumBucketCapacity;
    private final int minimumBucketCapacity;
    private final boolean fixedCapacity;
    private final int loadFactor;
    private final int initialBucketCapacity;
    private final boolean deferredEncoding;

    private Buckets<Key,Value> buckets;

    public HashIndex(Paged paged, int page, HashIndexFactory<Key,Value> factory) {
        traceStart(LOG, "HashIndex(%s, %d, %s)", paged.getClass(), page, factory);
        this.paged = paged;
        this.page = page;
        this.maximumBucketCapacity = factory.getMaximumBucketCapacity();
        this.minimumBucketCapacity = factory.getMinimumBucketCapacity();
        this.loadFactor = factory.getLoadFactor();
        this.deferredEncoding = factory.isDeferredEncoding();
        this.initialBucketCapacity = factory.getBucketCapacity();
        this.BIN_FACTORY.setKeyCodec(factory.getKeyCodec());
        this.BIN_FACTORY.setValueCodec(factory.getValueCodec());
        this.BIN_FACTORY.setDeferredEncoding(this.deferredEncoding);
        this.fixedCapacity = this.minimumBucketCapacity==this.maximumBucketCapacity && this.maximumBucketCapacity==this.initialBucketCapacity;
        traceEnd(LOG, "HashIndex");
    }

    public HashIndex<Key, Value> create() {
        traceStart(LOG, "HashIndex.create()");
        buckets = new Buckets<Key, Value>(this);
        buckets.create(initialBucketCapacity);
        storeBuckets();
        traceEnd(LOG, "HashIndex.create");
        return this;
    }

    public HashIndex<Key, Value> open() {
        traceStart(LOG, "HashIndex.open()");
        loadBuckets();
        traceEnd(LOG, "HashIndex.open");
        return this;
    }


    public Value get(Key key) {
        return buckets.bucket(key).get(key);
    }

    public boolean containsKey(Key key) {
        return buckets.bucket(key).containsKey(key);
    }

    public Value put(Key key, Value value) {
        traceStart(LOG, "HashIndex.put(%s, %s)", key, value);

        Index<Key, Value> indexBucket = buckets.bucket(key);

        if( fixedCapacity ) {
            Value put = indexBucket.put(key,value);
            traceEnd(LOG, "HashIndex.put -> %s", put);
            return put;
        }

        boolean wasEmpty = indexBucket.isEmpty();
        Value put = indexBucket.put(key,value);

        if (wasEmpty) {
            buckets.active++;
            storeBuckets();
        }

        if (buckets.active >= buckets.increaseThreshold) {
            int capacity = Math.min(this.maximumBucketCapacity, buckets.capacity * 4);
            if (buckets.capacity != capacity) {
                this.changeCapacity(capacity);
            }
        }
        traceEnd(LOG, "HashIndex.put -> %s", put);
        return put;
    }

    public Value putIfAbsent(Key key, Value value) {

        Index<Key, Value> indexBucket = buckets.bucket(key);

        if( fixedCapacity ) {
            return indexBucket.putIfAbsent(key,value);
        }

        boolean wasEmpty = indexBucket.isEmpty();
        Value put = indexBucket.putIfAbsent(key,value);

        if (wasEmpty) {
            buckets.active++;
            storeBuckets();
        }

        if (buckets.active >= buckets.increaseThreshold) {
            int capacity = Math.min(this.maximumBucketCapacity, buckets.capacity * 4);
            if (buckets.capacity != capacity) {
                this.changeCapacity(capacity);
            }
        }
        return put;
    }

    public Value remove(Key key) {
        traceStart(LOG, "HashIndex.remove(%s)", key);
        Index<Key, Value> indexBucket = buckets.bucket(key);

        if( fixedCapacity ) {
            return indexBucket.remove(key);
        }

        boolean wasEmpty = indexBucket.isEmpty();
        Value rc = indexBucket.remove(key);
        boolean isEmpty = indexBucket.isEmpty();

        if (!wasEmpty && isEmpty) {
            buckets.active--;
            storeBuckets();
        }

        if (buckets.active <= buckets.decreaseThreshold) {
            int capacity = Math.max(minimumBucketCapacity, buckets.capacity / 2);
            if (buckets.capacity != capacity) {
                changeCapacity(capacity);
            }
        }
        traceEnd(LOG, "HashIndex.remove -> %s", rc);
        return rc;
    }

    public void clear() {
        buckets.clear();
        if (buckets.capacity!=initialBucketCapacity) {
            changeCapacity(initialBucketCapacity);
        }
    }

    public int size() {
        int rc=0;
        for (int i = 0; i < buckets.capacity; i++) {
            rc += buckets.bucket(i).size();
        }
        return rc;
    }

    public boolean isEmpty() {
        return buckets.active==0;
    }

    public void destroy() {
        buckets.destroy();
        buckets = null;
        paged.free(page);
    }

    public int getIndexLocation() {
        return page;
    }

    // /////////////////////////////////////////////////////////////////
    // Helper methods Methods
    // /////////////////////////////////////////////////////////////////
    private void changeCapacity(final int capacity) {
        traceStart(LOG, "HashIndex.changeCapacity(%s)", capacity);
        debug("Resizing to: %d", capacity);

        Buckets<Key, Value> next = new Buckets<Key, Value>(this);
        next.create(capacity);

        // Copy the data from the old buckets to the new buckets.
        for (int i = 0; i < buckets.capacity; i++) {
            SortedIndex<Key, Value> bin = buckets.bucket(i);
            HashSet<Integer> activeBuckets = new HashSet<Integer>();
            for (Map.Entry<Key, Value> entry : bin) {
                Key key = entry.getKey();
                Value value = entry.getValue();
                Index<Key, Value> bucket = next.bucket(key);
                bucket.put(key, value);
                if( activeBuckets.add(bucket.getIndexLocation()) ) {
                    next.active++;
                }
            }
        }

        buckets.destroy();
        buckets = next;
        storeBuckets();

        debug("Resizing done.");
        traceEnd(LOG, "HashIndex.changeCapacity");
    }

    public String toString() {
        return "{ page: "+page+", buckets: "+buckets+" }";
    }

    private void storeBuckets() {
        traceStart(LOG, "HashIndex.storeBuckets()");
        if( deferredEncoding ) {
            paged.put(BUCKET_PAGED_ACCESSOR, page, buckets);
        } else {
            BUCKET_PAGED_ACCESSOR.store(paged, page, buckets);
        }
        traceEnd(LOG, "HashIndex.storeBuckets");
    }

    private void loadBuckets() {
        traceStart(LOG, "HashIndex.loadBuckets()");
        if( deferredEncoding ) {
            buckets = paged.get(BUCKET_PAGED_ACCESSOR, page);
        } else {
            buckets = BUCKET_PAGED_ACCESSOR.load(paged, page);
        }
        // TODO: free bucket pages
        traceEnd(LOG, "HashIndex.loadBuckets");
    }

    // /////////////////////////////////////////////////////////////////
    // Helper classes
    // /////////////////////////////////////////////////////////////////

    /**
     * This is the data stored in the index header.  It knows where
     * the hash buckets are stored at an keeps usage statistics about
     * those buckets.
     */
    static private class Buckets<Key,Value> {

        final HashIndex<Key,Value> index;
        int active;
        int capacity;
        int[] bucketsIndex;

        int increaseThreshold;
        int decreaseThreshold;

        public Buckets(HashIndex<Key, Value> index) {
            this.index = index;
        }

        private void calcThresholds() {
            increaseThreshold = (capacity * index.loadFactor)/100;
            decreaseThreshold = (capacity * index.loadFactor * index.loadFactor ) / 20000;
        }

        void create(int capacity) {
            traceStart(LOG, "Buckets.create(%d)", capacity);
            this.active = 0;
            this.capacity = capacity;
            this.bucketsIndex = new int[capacity];
            for (int i = 0; i < capacity; i++) {
                this.bucketsIndex[i] = index.BIN_FACTORY.create(index.paged).getIndexLocation();
            }
            calcThresholds();
            traceEnd(LOG, "Buckets.create");
        }

        public void destroy() {
            clear();
            for (int i = 0; i < capacity; i++) {
            index.paged.allocator().free(bucketsIndex[i], 1);
            }
        }

        public void clear() {
            for (int i = 0; i < index.buckets.capacity; i++) {
                index.buckets.bucket(i).clear();
            }
            index.buckets.active = 0;
            index.buckets.calcThresholds();
        }

        SortedIndex<Key,Value> bucket(int bucket) {
            traceStart(LOG, "Buckets.bucket(%d)", bucket);
            SortedIndex<Key, Value> ret = index.BIN_FACTORY.open(index.paged, bucketsIndex[bucket]);
            traceEnd(LOG, "Buckets.bucket -> %s", ret);
            return ret;
        }

        SortedIndex<Key,Value> bucket(Key key) {
            traceStart(LOG, "Buckets.bucket(%s)", key);

            int i = index(key);
            trace(LOG, "looking at index %d", i);
            SortedIndex<Key, Value> ret = index.BIN_FACTORY.open(index.paged, bucketsIndex[i]);
            traceEnd(LOG, "Buckets.bucket -> %s", ret);
            return ret;
        }

        int index(Key x) {
            return Math.abs(x.hashCode()%capacity);
        }

        @Override
        public String toString() {
            return "{ capacity: "+capacity+", active: "+active+", increase threshold: "+increaseThreshold+", decrease threshold: "+decreaseThreshold+" }";
        }

    }

    public static final Buffer MAGIC = new Buffer(new byte[] {'h', 'a', 's', 'h'});

    private final PagedAccessor<Buckets<Key, Value>> BUCKET_PAGED_ACCESSOR = new PagedAccessor<Buckets<Key, Value>>() {

        public List<Integer> store(Paged paged, int page, Buckets<Key, Value> buckets) {
            traceStart(LOG, "BUCKET_PAGED_ACCESSOR.store(%s, %d, %s)", paged, page, buckets);

            int bucketsSize =
                      MAGIC.length
                    + 4                  // buckets.capacity
                    + buckets.capacity*4 // all buckets
                    + 4;                 // buckets.active

            ExtentOutputStream eos = new ExtentOutputStream(paged, page, (short)1, bucketsSize);
            DataOutputStream os = new DataOutputStream(eos);

            try {
                os.write(MAGIC.data);

                os.writeInt(buckets.capacity);
                for (int i =0; i < buckets.capacity; i++) {
                    os.writeInt(buckets.bucketsIndex[i]);
                }
                os.writeInt(buckets.active);
                os.close();
            } catch (IOException e) {
                throw new IOPagingException(e);
            }


            List<Integer> pages = eos.getPages().values();

            traceEnd(LOG, "BUCKET_PAGED_ACCESSOR.store -> %s", pages);
            return pages;
        }

        public Buckets<Key, Value> load(Paged paged, int page) {
            traceStart(LOG, "BUCKET_PAGED_ACCESSOR.load(%s, %d)", paged, page);
            Buckets<Key, Value> buckets = new Buckets<Key, Value>(HashIndex.this);

            ExtentInputStream eis = new ExtentInputStream(paged, page);
            DataInputStream is = new DataInputStream(eis);

            try {
                Buffer magic = new Buffer(MAGIC.length);
                is.readFully(magic.data);
                if (!magic.equals(MAGIC)) {
                    throw new IndexException("Not a hash page");
                }

                buckets.capacity = is.readInt();
                buckets.bucketsIndex = new int[buckets.capacity];
                for (int i =0; i < buckets.capacity; i++) {
                    buckets.bucketsIndex[i] = is.readInt();
                }
                buckets.active = is.readInt();
                is.close();
            } catch (IOException e) {
                throw new IOPagingException(e);
            }

            traceEnd(LOG, "BUCKET_PAGED_ACCESSOR.load -> %s", buckets);
            return buckets;
        }

        public List<Integer> pagesLinked(Paged paged, int page) {
            traceStart(LOG, "BUCKET_PAGED_ACCESSOR.pagesLinked()");
            List<Integer> ret = Collections.emptyList();
            traceEnd(LOG, "BUCKET_PAGED_ACCESSOR.pagesLinked");
            return ret;
        }

    };

}
