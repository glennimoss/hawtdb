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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.fusesource.hawtdb.api.Paged;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtbuf.Buffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentOutputStream extends OutputStream {

    private final static Log LOG = LogFactory.getLog(ExtentOutputStream.class);

    private static final short DEFAULT_EXTENT_SIZE = 128; // 128 * 4k = .5MB
    private Paged paged;
    private short extentSize;
    private short nextExtentSize;
    private int page;
    private Extent current;
    private Ranges pages = new Ranges();


    public ExtentOutputStream(Paged paged) {
        this(paged, DEFAULT_EXTENT_SIZE);
    }

    /*
    public ExtentOutputStream(Paged paged, short extentSize) {
        init(paged, paged.allocator().alloc(extentSize), extentSize, extentSize);
    }

    public ExtentOutputStream(Paged paged, short extentSize, short nextExtentSize) {
        init(paged, paged.allocator().alloc(extentSize), extentSize, nextExtentSize);
    }
    */

    public ExtentOutputStream(Paged paged, int size) {
        short extentSize = DEFAULT_EXTENT_SIZE;
        if (size > -1) {
          extentSize = (short)paged.pages(size + Extent.DEFAULT_MAGIC.length + 8);
        }

        init(paged, paged.allocator().alloc(extentSize), extentSize, extentSize);
    }

    public ExtentOutputStream(Paged paged, int page, short extentSize, int size) {
        traceStart(LOG, "ExtentOutputStream(%s, %d, %d, %d)", paged.getClass(), page, extentSize, size);
        int extentHeader = Extent.DEFAULT_MAGIC.length + 8;
        // take away amount that can be stored in the first extent
        size -= paged.getPageSize() * extentSize - extentHeader;
        short nextExtentSize = DEFAULT_EXTENT_SIZE;
        // if it can't all fit in the provided extent...
        if (size > 0) {
          // with what's left, see how long an extent is necessary to hold it all
          nextExtentSize = (short)paged.pages(size + extentHeader);
        }
        trace(LOG, "nextExtentSize = %d", nextExtentSize);

        init(paged, page, extentSize, nextExtentSize);
        traceEnd(LOG, "ExtentOutputStream");
    }

    //public ExtentOutputStream(Paged paged, int page, short extentSize, short nextExtentSize) {
        //init(paged, page, extentSize, nextExtentSize);
    //}

    private void init(Paged paged, int page, short extentSize, short nextExtentSize) {
        traceStart(LOG, "ExtentOutputStream.init(paged: %s, page: %d, extentSize: %d, nextExtentSize: %d)",
            paged, page, extentSize, nextExtentSize);
        if (extentSize == 1 && nextExtentSize == 1 && paged.getPageSize() == Extent.DEFAULT_MAGIC.length + 8) {
            throw new IllegalArgumentException(
                "Pathological case: extentSize, nextExtentSize = 1 " +
                "and Paged's pageSize = Extent.DEFAULT_MAGIC.length + 8");
        }
        this.paged = paged;
        this.extentSize = extentSize;
        this.nextExtentSize = nextExtentSize;
        this.page = page;
        current = new Extent(paged, page);
        current.writeOpen(extentSize);
        traceEnd(LOG, "ExtentOutputStream.init");
    }

    @Override
    public String toString() {
        return "{ page: "+page+", extent size: "+extentSize+
                ", next extent size: "+nextExtentSize+" current: "+current+" }";
    }

    public void write(int b) throws IOException {
        traceStart(LOG, "ExtentOutputStream.write(%02X)", b);
        while (!current.write((byte) b)) {
            current = nextExtent();
        }
        traceEnd(LOG, "ExtentOutputStream.write");
    }

    @Override
    public void write(byte[] b, int off, int len) {
        traceStart(LOG, "ExtentOutputStream.write()");
        Buffer buffer = new Buffer(b, off, len);
        trace(LOG, "buffer = %s", buf(buffer));
        while (buffer.length > 0) {
            if (!current.write(buffer)) {
                current = nextExtent();
            }
        }
        traceEnd(LOG, "ExtentOutputStream.write");
    }

    protected Extent nextExtent() {
        traceStart(LOG, "ExtentOutputStream.nextExtent()");
        int nextPageId = this.paged.allocator().alloc(nextExtentSize);
        current.writeCloseLinked(nextPageId);
        pages.add(current.getPage(), paged.pages(current.getLength()));
        Extent nextExtent = new Extent(paged, nextPageId);
        nextExtent.writeOpen(nextExtentSize);

        traceEnd(LOG, "ExtentOutputStream.nextExtent -> %s", nextExtent);
        return nextExtent;
    }

    public short getExtentSize() {
        return extentSize;
    }

    public short getNextExtentSize() {
        return nextExtentSize;
    }

    public int getPage() {
        return page;
    }

    @Override
    public void close(){
        traceStart(LOG, "ExtentOutputStream.close()");
        current.writeCloseEOF();
        pages.add(current.getPage(), paged.pages(current.getLength()));
        traceEnd(LOG, "ExtentOutputStream.close");
    }

    public Ranges getPages() {
        return pages;
    }
}
