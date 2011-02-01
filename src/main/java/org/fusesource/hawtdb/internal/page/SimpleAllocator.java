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

import java.util.Iterator;

import org.fusesource.hawtdb.api.Allocator;
import org.fusesource.hawtdb.api.OutOfSpaceException;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtdb.internal.util.Ranges.Range;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;
import static org.fusesource.hawtdb.internal.page.Logging.traced;


/**
 * This class is used to provides allocation management of pages.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class SimpleAllocator implements Allocator {

    private final static Log LOG = LogFactory.getLog(SimpleAllocator.class);

    private final Ranges freeRanges = new Ranges();
    private int limit;

    public SimpleAllocator(int limit) {
        trace(LOG, "SimpleAllocator(%d)", limit);
        this.limit = limit;
        freeRanges.add(0, limit);
    }

    private void op_trace(String op, int page, int count) {
        if( traced(page) ) {
            trace(LOG, "%s: %d : %d", op, page, count);
        }
    }


    /**
     * @see Allocator#alloc(int)
     */
    synchronized public int alloc(int size) throws OutOfSpaceException {
        traceStart(LOG, "SimpleAllocator.alloc(%d)", size);
        for (Iterator<Range> i = freeRanges.iterator(); i.hasNext();) {
            Range r = (Range) i.next();
            if( r.size() >= size ) {
                int rc = r.start;
                op_trace("ALLOC", rc, size);
                freeRanges.remove(rc, size);
                trace(LOG, "Allocated %d pages starting at %d", size, rc);
                traceEnd(LOG, "SimpleAllocator.alloc -> %d", rc);
                return rc;
            }
        }
        traceEnd(LOG, "SimpleAllocator.alloc -> OutOfSpaceException");
        throw new OutOfSpaceException();
    }


    /**
     * @see Allocator#free(int, int)
     */
    synchronized public void free(int pageId, int count) {
        trace(LOG, "SimpleAllocator.free(%d, %d)", pageId, count);
        freeRanges.add(pageId, count);
        op_trace("FREE", pageId, count);
    }

    /**
     * @see Allocator#unfree(int, int)
     */
    synchronized public void unfree(int pageId, int count) {
        trace(LOG, "SimpleAllocator.unfree(%d, %d)", pageId, count);
        freeRanges.remove(pageId, count);
    }

    synchronized public void clear() throws UnsupportedOperationException {
        freeRanges.clear();
        freeRanges.add(0, limit);
    }

    synchronized public void setFreeRanges(Ranges freePages) throws UnsupportedOperationException {
        freeRanges.copy(freePages);
    }

    public int getLimit() {
        return limit;
    }

    public boolean isAllocated(int page) {
        boolean ret = !freeRanges.contains(page);
        trace(LOG, "SimpleAllocator.isAllocated(%d) -> %b", page, ret);
        return ret;
    }

    public Ranges getFreeRanges() {
        return freeRanges;
    }

    @Override
    public String toString() {
        return "{ free pages: "+freeRanges.toString()+" }";
    }

}
