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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.index.BTreeNode.Data;
import org.fusesource.hawtdb.internal.page.Extent;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * A variable magnitude b+tree indexes with support for optional
 * simple-prefix optimization.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndex<Key, Value> implements SortedIndex<Key, Value> {

    private final static Log LOG = LogFactory.getLog(BTreeIndex.class);

    private final BTreeNode.DataPagedAccessor<Key, Value> DATA_ENCODER_DECODER =
        new BTreeNode.DataPagedAccessor<Key, Value>(this);

    private final Paged paged;
    private final int page;
    private final Codec<Key> keyCodec;
    private final Codec<Value> valueCodec;
    private final Prefixer<Key> prefixer;
    private final boolean deferredEncoding;
    private final Comparator comparator;

    public BTreeIndex(Paged paged, int page, BTreeIndexFactory<Key, Value> factory) {
        traceStart(LOG, "BTreeIndex.BTreeIndex(%s, %d, %s)", paged.getClass(), page, factory);
        this.paged = paged;
        this.page = page;
        this.keyCodec = factory.getKeyCodec();
        this.valueCodec = factory.getValueCodec();

        trace(LOG, "factory.isDeferredEncoding: %b", factory.isDeferredEncoding());
        trace(LOG, "keyCodec.isEstimatedSizeSupported: %b", keyCodec.isEstimatedSizeSupported());
        trace(LOG, "keyCodec.getFixedSize: %d", keyCodec.getFixedSize());
        trace(LOG, "valueCodec.isEstimatedSizeSupported: %b", valueCodec.isEstimatedSizeSupported());
        trace(LOG, "valueCodec.getFixedSize: %d", valueCodec.getFixedSize());
        // Deferred encoding can only done if the keys and value sizes can be computed.
        this.deferredEncoding = factory.isDeferredEncoding() &&
                ( keyCodec.isEstimatedSizeSupported() || keyCodec.getFixedSize()>=0 ) &&
                ( valueCodec.isEstimatedSizeSupported() || valueCodec.getFixedSize()>=0 );
        trace(LOG, "deferredEncoding = %b", this.deferredEncoding);

        this.prefixer = factory.getPrefixer();
        this.comparator = factory.getComparator();
        traceEnd(LOG, "BTreeIndex.BTreeIndex");
    }

    @Override
    public String toString() {
        return "{ page: "+page+", deferredEncoding: "+deferredEncoding+" }";
    }

    public void create() {
        traceStart(LOG, "BTreeIndex.create()");
        // Store the root page..
        BTreeNode<Key, Value> root = new BTreeNode<Key, Value>(null, page);
        storeNode(root);
        traceEnd(LOG, "BTreeIndex.create");
    }

    public boolean containsKey(Key key) {
        traceStart(LOG, "BTreeIndex.containsKey(%s)", key);
        boolean ret = root().contains(this, key);
        traceEnd(LOG, "BTreeIndex.containsKey -> %b", ret);
        return ret;
    }

    public Value get(Key key) {
        traceStart(LOG, "BTreeIndex.get(%s)", key);
        Value ret = root().get(this, key);
        traceEnd(LOG, "BTreeIndex.get -> %s", ret);
        return ret;
    }

    public Value put(Key key, Value value) {
        traceStart(LOG, "BTreeIndex.put(%s, %s)", key, value);
        Value ret = root().put(this, key, value);
        traceEnd(LOG, "BTreeIndex.put -> %s", ret);
        return ret;
    }

    public Value putIfAbsent(Key key, Value value) {
        traceStart(LOG, "BTreeIndex.putIfAbsent(%s, %s)", key, value);
        Value ret = root().putIfAbsent(this, key, value);
        traceEnd(LOG, "BTreeIndex.putIfAbsent -> %s", ret);
        return ret;
    }

    public Value remove(Key key) {
        traceStart(LOG, "BTreeIndex.remove(%s)", key);
        Value ret = root().remove(this, key);
        traceEnd(LOG, "BTreeIndex.remove -> %s", ret);
        return ret;
    }

    public int size() {
        traceStart(LOG, "BTreeIndex.size()");
        int ret = root().size(this);
        traceEnd(LOG, "BTreeIndex.size -> %d", ret);
        return ret;
    }

    public boolean isEmpty() {
        traceStart(LOG, "BTreeIndex.isEmpty()");
        boolean ret = root().isEmpty(this);
        traceEnd(LOG, "BTreeIndex.isEmpty -> %b", ret);
        return ret;
    }

    public void clear() {
        traceStart(LOG, "BTreeIndex.clear()");
        root().clear(this);
        traceEnd(LOG, "BTreeIndex.clear");
    }

    public int getMinLeafDepth() {
        traceStart(LOG, "BTreeIndex.getMinLeafDepth()");
        int ret = root().getMinLeafDepth(this, 0);
        traceEnd(LOG, "BTreeIndex.getMinLeafDepth -> %d", ret);
        return ret;
    }

    public int getMaxLeafDepth() {
        traceStart(LOG, "BTreeIndex.getMaxLeafDepth()");
        int ret = root().getMaxLeafDepth(this, 0);
        traceEnd(LOG, "BTreeIndex.getMaxLeafDepth -> %d", ret);
        return ret;
    }

    public void printStructure(PrintWriter out) {
        traceStart(LOG, "BTreeIndex.printStructure()");
        root().printStructure(this, out, "", "");
        traceEnd(LOG, "BTreeIndex.printStructure");
    }

    public void printStructure(OutputStream out) {
        traceStart(LOG, "BTreeIndex.printStructure()");
        PrintWriter pw = new PrintWriter(out, false);
        root().printStructure(this, pw, "", "");
        pw.flush();
        traceEnd(LOG, "BTreeIndex.printStructure");
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return root().iterator(this);
    }

    public Iterator<Map.Entry<Key, Value>> iterator(Predicate<Key> predicate) {
        return root().iterator(this, predicate);
    }

    public Iterator<Map.Entry<Key, Value>> iterator(final Key initialKey) {
        return root().iterator(this, initialKey);
    }

    public void visit(IndexVisitor<Key, Value> visitor) {
        root().visit(this, visitor);
    }

    public Map.Entry<Key, Value> getFirst() {
        return root().getFirst(this);
    }

    public Map.Entry<Key, Value> getLast() {
        return root().getLast(this);
    }

    // /////////////////////////////////////////////////////////////////
    // Internal implementation methods
    // /////////////////////////////////////////////////////////////////
    private BTreeNode<Key, Value> root() {
        traceStart(LOG, "BTreeIndex.root()");
        BTreeNode<Key, Value> root = loadNode(null, page);
        traceEnd(LOG, "BTreeIndex.root -> %s", root);
        return root;
    }

    // /////////////////////////////////////////////////////////////////
    // Internal methods made accessible to BTreeNode
    // /////////////////////////////////////////////////////////////////
    BTreeNode<Key, Value> createNode(BTreeNode<Key, Value> parent, Data<Key, Value> data) {
        traceStart(LOG, "BTreeIndex.createNode(%s, %s)", parent, data);
        BTreeNode<Key, Value> ret = new BTreeNode<Key, Value>(parent, paged.allocator().alloc(1), data);
        traceEnd(LOG, "BTreeIndex.createNode -> %s", ret);
        return ret;
    }

    BTreeNode<Key, Value> createNode(BTreeNode<Key, Value> parent) {
        traceStart(LOG, "BTreeIndex.createNode(%s)", parent);
        BTreeNode<Key, Value> ret = new BTreeNode<Key, Value>(parent, paged.allocator().alloc(1));
        traceEnd(LOG, "BTreeIndex.createNode -> %s", ret);
        return ret;
    }

    @SuppressWarnings("serial")
    static class PageOverflowIOException extends IndexException {
    }

    /**
     *
     * @param node
     * @return false if page overflow occurred
     */
    boolean storeNode(BTreeNode<Key, Value> node) {
        traceStart(LOG, "BTreeIndex.storeNode(%s)", node);
        if (deferredEncoding) {
            trace(LOG, "deferred encoding");
            int size = BTreeNode.estimatedSize(this, node.data);
            size += 9; // The extent header.
            trace(LOG, "node size %d bytes", size);

            if (!node.allowPageOverflow() && size>paged.getPageSize()) {
                trace(LOG, "no overflow allowed and this [%d] is too big for the page [%d]", size, paged.getPageSize());
                traceEnd(LOG, "BTreeIndex.storeNode -> false");
                return false;
            }

            paged.put(DATA_ENCODER_DECODER, node.getPage(), node.data);
            node.storedInExtent=true;
        } else {
            trace(LOG, "not deferred encoding");

            if (node.storedInExtent) {
                // TODO: not getting the results therefore expecting free?
                List<Integer> freed = DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
                trace(LOG, "Stored in extent, freeing pages linked to %d: %s", node.page, freed);
            }

            if (node.isLeaf()) {
                trace(LOG, "node is leaf");
                List<Integer> pages = DATA_ENCODER_DECODER.store(paged, node.page, node.data);
                // TODO: this pages.size() is actually the number of LINKED
                // pages, so this will only split when the node is over 2
                // pages in size.
                if( !node.allowPageOverflow() && pages.size()>1 ) {
                    trace(LOG, "node should not have overflowed?");
                    // TODO: not getting the results therefore expecting free?
                    DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
                    node.storedInExtent=false;
                    traceEnd(LOG, "BTreeIndex.storeNode -> false");
                    return false;
                }
                node.storedInExtent=true;
            } else {
                // TODO: Why treat this differently than the leaf in terms of
                // using an Extent?
                trace(LOG, "node is branch");
                DataByteArrayOutputStream os = new DataByteArrayOutputStream(paged.getPageSize()) {
                    protected void resize(int newcount) {
                        trace(LOG, "Refusing to resize");
                        throw new PageOverflowIOException();
                    };
                };
                try {
                    BTreeNode.write(os, this, node.data);
                    paged.write(node.page, os.toBuffer());
                    node.storedInExtent=false;
                } catch (IOException e) {
                    traceEnd(LOG, "BTreeIndex.storeNode -> IndexException");
                    throw new IndexException("Could not write btree node");
                } catch (PageOverflowIOException e) {
                    traceEnd(LOG, "BTreeIndex.storeNode -> false");
                    return false;
                }
            }
        }
        traceEnd(LOG, "BTreeIndex.storeNode -> true");
        return true;
    }



    BTreeNode<Key, Value> loadNode(BTreeNode<Key, Value> parent, int page) {
        traceStart(LOG, "BTreeIndex.loadNode(%s, %d)", parent, page);
        BTreeNode<Key, Value> node = new BTreeNode<Key, Value>(parent, page);
        if( deferredEncoding ) {
            trace(LOG, "deferred encoding; assuming stored in extent");
            node.data = paged.get(DATA_ENCODER_DECODER, page);
            node.storedInExtent=true;
        } else {
            trace(LOG, "not deferred encoding");
            Buffer buffer = new Buffer(paged.getPageSize());
            paged.read(page, buffer);
            if ( buffer.startsWith(Extent.DEFAULT_MAGIC) ) {
                trace(LOG, "stored in extent");
                // Page data was stored in an extent..
                node.data = DATA_ENCODER_DECODER.load(paged, page);
                node.storedInExtent=true;
            } else {
                trace(LOG, "plain page");
                // It was just in a plain page..
                // TODO: can/should this ever happen?
                // A: Apparently if it's not deferred, and is a leaf, no
                // extent is used
                DataByteArrayInputStream is = new DataByteArrayInputStream(buffer);
                try {
                    node.data = BTreeNode.read(is, this);
                    node.storedInExtent=false;
                } catch (IOException e) {
                    traceEnd(LOG, "BTreeIndex.loadNode -> could not read btree node");
                    throw new IndexException("Could not read btree node");
                }
            }
        }
        traceEnd(LOG, "BTreeIndex.loadNode -> %s", node);
        return node;
    }

    void free( BTreeNode<Key, Value> node ) {
        traceStart(LOG, "BTreeIndex.free(%s)", node);
        if( deferredEncoding ) {
            trace(LOG, "deferred encoding, freeing linked pages");
            paged.clear(DATA_ENCODER_DECODER, node.page);
        } else {
            trace(LOG, "deferred encoding");
            if (node.storedInExtent) {
                trace(LOG, "stored in extent, freeing linked pages");
                // TODO: not getting the results therefore expecting free?
                DATA_ENCODER_DECODER.pagesLinked(paged, node.page);
            }
        }

        trace(LOG, "freeing node page %d", node.page);
        paged.free(node.page);
        traceEnd(LOG, "BTreeIndex.free");
    }

    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////

    public Paged getPaged() {
        return paged;
    }

    public int getIndexLocation() {
        return page;
    }

    public Codec<Key> getKeyMarshaller() {
        return keyCodec;
    }

    public Codec<Value> getValueMarshaller() {
        return valueCodec;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void destroy() {
        clear();
        paged.free(page);
    }


}
