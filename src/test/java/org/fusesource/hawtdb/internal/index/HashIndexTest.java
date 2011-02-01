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
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

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

    /*
    @Override
    public void testRandomRemove() throws Exception {
        // TODO: look into why this test is failing.
    }
    */
}
