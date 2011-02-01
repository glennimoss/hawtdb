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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.fusesource.hawtdb.api.Paged;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtbuf.Buffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * An InputStream which reads it's data from an
 * extent previously written with the {@link ExtentOutputStream}.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentInputStream extends InputStream {

    private final static Log LOG = LogFactory.getLog(ExtentInputStream.class);

    private final Paged paged;
    private Extent current;
    private final int page;
    private Ranges pages = new Ranges();

    public ExtentInputStream(Paged paged, int page) {
        traceStart(LOG, "ExtentInputStream(%s, %d)", paged, page);
        this.paged = paged;
        this.page = page;
        current = new Extent(paged, page);
        current.readOpen();
        pages.add(current.getPage(), paged.pages(current.getLength()));
        traceEnd(LOG, "ExtentInputStream");
    }

    @Override
    public String toString() {
        return "{ page: "+page+", current: "+current+" }";
    }


    @Override
    public int read() throws IOException {
        traceStart(LOG, "ExtentInputStream.read()");
        if (current == null) {
            traceEnd(LOG, "ExtentInputStream.read -> -1");
            return -1;
        }
        if (current.atEnd()) {
            current = nextExtent();
            if (current == null) {
                traceEnd(LOG, "ExtentInputStream.read -> -1");
                return -1;
            }
        }
        int b = current.read();
        traceEnd(LOG, "ExtentInputStream.read -> %02X", b);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        traceStart(LOG, "ExtentInputStream.read(byte[%d], %d, %d)", b.length, off, len);
        int rc = len;
        Buffer buffer = new Buffer(b, off, len);
        trace(LOG, "buffer = %s", buf(buffer));
        if( current == null ) {
            traceEnd(LOG, "ExtentInputStream.read -> -1");
            return -1;
        }
        while (buffer.length > 0) {
            if (current.atEnd()) {
                current = nextExtent();
                if (current == null) {
                    break;
                }
            }
            current.read(buffer);
        }
        rc -= buffer.length;
        if (rc == 0) {
            traceEnd(LOG, "ExtentInputStream.read -> -1");
            return -1;
        }
        traceEnd(LOG, "ExtentInputStream.read -> %d", rc);
        return rc;
    }

    protected Extent nextExtent() {
        traceStart(LOG, "ExtentInputStream.nextExtent()");
        int next = current.getNext();
        current.readClose();
        if (next == -1) {
            return null;
        }
        Extent nextExtent = new Extent(paged, next);
        nextExtent.readOpen();
        pages.add(nextExtent.getPage(), paged.pages(nextExtent.getLength()));
        traceEnd(LOG, "ExtentInputStream.nextExtent -> %s", nextExtent);
        return nextExtent;
    }

    @Override
    public void close() throws IOException {
        traceStart(LOG, "ExtentInputStream.close()");
        if (current != null) {
            current.readClose();
            current = null;
        }
        traceEnd(LOG, "ExtentInputStream.close");
    }

    public Ranges getPages() {
        return pages;
    }
}
