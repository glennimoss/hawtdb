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
package org.fusesource.hawtdb.api;

import org.fusesource.hawtdb.internal.page.Extent;
import org.fusesource.hawtdb.internal.page.ExtentInputStream;
import org.fusesource.hawtdb.internal.page.ExtentOutputStream;
import org.fusesource.hawtdb.internal.util.Ranges;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * Abstract base class for implementations of PagedAccessor which use Extents.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class AbstractStreamPagedAccessor<T>  implements PagedAccessor<T> {

    private final static Log LOG = LogFactory.getLog(AbstractStreamPagedAccessor.class);

    public List<Integer> store(Paged paged, int page, T data) {
        traceStart(LOG, "AbstractStreamPagedAccessor.store(%s, %d, %s)", paged, page, data);
        // The node will be stored in an extent. This allows us to easily
        // support huge nodes.
        // The first extent is only 1 page long, extents linked off.
        // The linked extents will be long enough to hold the data, if its
        // size can be estimated, otherwise 128 pages.
        ExtentOutputStream eos = new ExtentOutputStream(paged, page, (short) 1, estimateSize(data));
        DataOutputStream os = new DataOutputStream(eos);
        try {
            encode(paged, os, data);
            os.close();
        } catch (IOException e) {
            throw new IndexException(e);
        }

        Ranges pages = eos.getPages();
        pages.remove(page);
        if (pages.isEmpty()) {
            traceEnd(LOG, "AbstractStreamPagedAccessor.store -> []");
            return Collections.emptyList();
        }

        List<Integer> ret =  pages.values();
        traceEnd(LOG, "AbstractStreamPagedAccessor.store -> %s", ret);
        return ret;
    }

    public T load(Paged paged, int page) {
        ExtentInputStream eis = new ExtentInputStream(paged, page);
        DataInputStream is = new DataInputStream(eis);
        try {
            return decode(paged, is);
        } catch (IOException e) {
            throw new IndexException(e);
        } finally {
            try {
                is.close();
            } catch (Throwable ignore) {
            }
        }

    }

    public List<Integer> pagesLinked(Paged paged, int page) {
        return Extent.pagesLinked(paged, page);
    }


    abstract protected void encode(Paged paged, DataOutputStream os, T data) throws IOException;
    abstract protected T decode(Paged paged, DataInputStream is) throws IOException;
    protected int estimateSize(T data) {
      return -1;
    }
}
