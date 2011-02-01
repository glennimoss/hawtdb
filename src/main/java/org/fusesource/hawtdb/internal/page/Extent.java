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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.fusesource.hawtdb.api.IOPagingException;
import org.fusesource.hawtdb.api.Paged;
import org.fusesource.hawtdb.api.Paged.SliceType;
import org.fusesource.hawtbuf.Buffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * An extent is a sequence of adjacent pages which can be linked
 * to subsequent extents.
 *
 * Extents allow you to write large streams of data to a Paged object
 * contiguously to avoid fragmentation.
 *
 * The first page of the extent contains a header which specifies
 * the size of the extent and the page id of the next extent that
 * it is linked to.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Extent {

    private final static Log LOG = LogFactory.getLog(Extent.class);

    public final static Buffer DEFAULT_MAGIC = new Buffer(new byte[]{'x'});

    private final Paged paged;
    private final int page;
    private final Buffer magic;

    private ByteBuffer buffer;

    private int length;
    private int next;

    public Extent(Paged paged, int page) {
        this(paged, page, DEFAULT_MAGIC);
    }

    public Extent(Paged paged, int page, Buffer magic) {
        trace(LOG, "Extent(%s, %d, %s)", paged, page, buf(magic));
        if (paged.getPageSize() < magic.length + 8) {
            throw new IllegalArgumentException(
                    String.format("Paged's pageSize [%d] must be greater than magic.length + 8 [%d]",
                        paged.getPageSize(), magic.length + 8));
        };
        this.paged = paged;
        this.page = page;
        this.magic = magic;
    }

    @Override
    public String toString() {
        Integer position = null;
        Integer limit = null;
        if( buffer!=null ) {
            position = buffer.position();
            limit = buffer.limit();
        }
        return "{ page: "+page+", position: "+position+", limit: "+limit+", length: "+length+", next: "+next+" }";
    }


    public void readHeader() {
        traceStart(LOG, "Extent.readHeader()");
        buffer = paged.slice(SliceType.READ, page, 1);
        trace(LOG, "buffer = %s", buffer);

        Buffer m = new Buffer(magic.length);
        buffer.get(m.data);
        trace(LOG, "magic = %s", buf(m));


        if( !magic.equals(m) ) {
            throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: " + page
                    + ". Magic: expected = " + hexify(magic) + " found = " + hexify(m));
        }

        IntBuffer ib = buffer.asIntBuffer();
        length = ib.get();
        next = ib.get();
        traceEnd(LOG, "Extent.readHeader");
    }

    public void readOpen() {
        readHeader();
        int pages = paged.pages(length);
        if( pages > 1 ) {
            paged.unslice(buffer);
            buffer = paged.slice(SliceType.READ, page, pages);
        }
        buffer.position(magic.length+8);
        buffer.limit(length);
    }

    public void writeOpen(short size) {
        trace(LOG, "Extent.writeOpen(%d)", size);
        assert size > 1 || paged.getPageSize() > magic.length + 8;
        buffer = paged.slice(SliceType.WRITE, page, size);
        buffer.position(magic.length+8);
    }

    public int writeCloseLinked(int next) {
        traceStart(LOG, "Extent.writeCloseLinked(%d)", next);
        this.next = next;
        length = buffer.position();
        buffer.position(0);
        buffer.put(magic.data, magic.offset, magic.length);
        IntBuffer ib = buffer.asIntBuffer();
        ib.put(length);
        ib.put(next);
        paged.unslice(buffer);
        traceEnd(LOG, "Extent.writeCloseLinked -> %d", length);
        return length;
    }

    public void writeCloseEOF() {
        traceStart(LOG, "Extent.writeCloseEOF()");
        int length = writeCloseLinked(-1);
        trace(LOG, "length = %d", length);
        int originalPages = paged.pages(buffer.limit());
        trace(LOG, "originalPages = %d", originalPages);
        int usedPages = paged.pages(length);
        trace(LOG, "usedPages = %d", usedPages);
        int remainingPages = originalPages-usedPages;
        trace(LOG, "remainingPages = %d", remainingPages);

        // Release un-used pages.
        if (remainingPages>0) {
            trace(LOG, "Freeing remaining pages");
            paged.allocator().free(page+usedPages, remainingPages);
        }
        paged.unslice(buffer);
        traceEnd(LOG, "Extent.writeCloseEOF");
    }

    public void readClose() {
        trace(LOG, "Extent.readClose()");
        paged.unslice(buffer);
    }

    boolean atEnd() {
        trace(LOG, "Extent.atEnd() remaining %d", buffer.remaining());
        return buffer.remaining() == 0;
    }

    /**
     * @return true if the write fit into the extent.
     */
    public boolean write(byte b) {
        traceStart(LOG, "Extent.write(%02X)", b);
        if (atEnd()) {
            traceEnd(LOG, "Extent.write -> false");
            return false;
        }
        buffer.put(b);
        traceEnd(LOG, "Extent.write -> true");
        return true;
    }

    public boolean write(Buffer source) {
        traceStart(LOG, "Extent.write({ offset: %d length: %d })",
                source.offset, source.length, Arrays.toString(source.data));
        while (source.length > 0) {
            if (atEnd()) {
                trace(LOG, "Exhausted this extent");
                traceEnd(LOG, "Extent.write -> false");
                return false;
            }
            int count = Math.min(buffer.remaining(), source.length);
            buffer.put(source.data, source.offset, count);
            source.offset += count;
            source.length -= count;
            trace(LOG, "buffer = { offset: %d length: %d }", source.offset, source.length, Arrays.toString(source.data));
        }
        trace(LOG, "Wrote all data");
        traceEnd(LOG, "Extent.write -> true");
        return true;
    }

    public int read() {
        int b = buffer.get() & 0xFF;
        trace(LOG, "Extent.read() -> %02X", b);
        return b;
    }

    public void read(Buffer target) {
        traceStart(LOG, "Extent.read(%s)", buf(target));
        while (target.length > 0 && !atEnd()) {
            int count = Math.min(buffer.remaining(), target.length);
            trace(LOG, "reading %d bytes", count);
            buffer.get(target.data, target.offset, count);
            trace(LOG, "target = %s", buf(target));
            target.offset += count;
            target.length -= count;
        }
        traceEnd(LOG, "Extent.read");
    }

    public int getNext() {
        return next;
    }

    /**
     * Gets a listing of all the pages used by the extent at the specified page.
     *
     * @param paged
     * @param page
     */
    public static List<Integer> pagesLinked(Paged paged, int page) {
        // TODO: ??? Should this call pagesLinked below rather that FREE?!?!
        return freeLinked(paged, page, DEFAULT_MAGIC);
    }

    public static List<Integer> pagesLinked(Paged paged, int page, Buffer magic) {
        traceStart(LOG, "Extent.pagesLinked(%s, %d, %s)", paged, page, buf(magic));
        Extent extent = new Extent(paged, page, magic);
        extent.readHeader();
        List<Integer> rc = pages(paged, extent.getNext());
        traceEnd(LOG, "Extent.pagesLinked -> %s", rc);
        return rc;
    }

    public static List<Integer> pages(Paged paged, int page) {
        return pages(paged, page, DEFAULT_MAGIC);
    }

    public static List<Integer> pages(Paged paged, int page, Buffer magic) {
        traceStart(LOG, "Extent.pages(%s, %d, %s)", paged, page, buf(magic));
        ArrayList<Integer> rc = new ArrayList<Integer>();
        while( page>=0 ) {
            Extent extent = new Extent(paged, page, magic);
            extent.readHeader();
            try {
                int pagesInExtent = paged.pages(extent.getLength());
                for( int i=0; i < pagesInExtent; i++) {
                    rc.add(page+i);
                }
                page=extent.getNext();
            } finally {
                extent.readClose();
            }
        }
        traceEnd(LOG, "Extent.pages -> %s", rc);
        return rc;
    }


    /**
     * Frees the linked extents at the provided page id.
     *
     * @param paged
     * @param page
     */
    public static List<Integer> freeLinked(Paged paged, int page) {
        return freeLinked(paged, page, DEFAULT_MAGIC);
    }

    public static List<Integer> freeLinked(Paged paged, int page, Buffer magic) {
        traceStart(LOG, "Extent.freeLinked(%s, %d, %s)", paged, page, buf(magic));
        Extent extent = new Extent(paged, page, magic);
        extent.readHeader();
        List<Integer> rc = free(paged, extent.getNext());
        traceEnd(LOG, "Extent.freeLinked -> %s", rc);
        return rc;
    }

    /**
     * Frees the extent at the provided page id.
     *
     * @param paged
     * @param page
     */
    public static List<Integer> free(Paged paged, int page) {
        return free(paged, page, DEFAULT_MAGIC);
    }
    public static List<Integer> free(Paged paged, int page, Buffer magic) {
        traceStart(LOG, "Extent.free(%s, %d, %s)", paged, page, buf(magic));
        ArrayList<Integer> rc = new ArrayList<Integer>();
        while( page>=0 ) {
            Extent extent = new Extent(paged, page, magic);
            extent.readHeader();
            try {
                int pagesInExtent = paged.pages(extent.getLength());
                paged.allocator().free(page, pagesInExtent);
                for( int i=0; i < pagesInExtent; i++) {
                    rc.add(page+i);
                }
                page=extent.getNext();
            } finally {
                extent.readClose();
            }
        }
        traceEnd(LOG, "Extent.free -> %s", rc);
        return rc;
    }

    /**
     * Un-frees the extent at the provided page id.  Basically undoes
     * a previous {@link #free(PageFile, int)} operation.
     *
     * @param paged
     * @param page
     */
    public static void unfree(Paged paged, int page) {
        unfree(paged, page, DEFAULT_MAGIC);
    }
    public static void unfree(Paged paged, int page, Buffer magic) {
        traceStart(LOG, "Extent.unfree(%s, %d, %s)", paged, page, buf(magic));
        while( page>=0 ) {
            Extent extent = new Extent(paged, page, magic);
            extent.readHeader();
            try {
                paged.allocator().unfree(page, paged.pages(extent.length));
                page=extent.next;
            } finally {
                extent.readClose();
            }
        }
        traceEnd(LOG, "Extent.unfree");
    }

    public int getPage() {
        return page;
    }

    public int getLength() {
        return length;
    }
}
