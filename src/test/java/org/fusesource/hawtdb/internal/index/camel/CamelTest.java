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
package org.fusesource.hawtdb.internal.index.camel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.IndexVisitor;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class CamelTest {
    private final static Log LOG = LogFactory.getLog(CamelTest.class);

    @Test
    public void testCamel() throws Exception {
        traceStart(LOG, "CamelTest.testCamel()");
        File f = new File("target/test-data/" + getClass().getName() + ".db");
        f.delete();

        Repository repo = new Repository("foo", f);

        for (int i=0; i < 10000; i++) {
            String key = key(i % 3);
            String old = repo.get(key);
            StringBuilder sb = new StringBuilder(old != null ? old : "");
            for (int j=0; j < 100; j++) {
                sb.append(String.format(">%04dxx%02d<", i, j));
            }

            if (old != null && old.length() < 5000) {
                trace(LOG, "Aggregated key [%s]: %s", key, sb.toString());
                repo.add(key, sb.toString());
            } else {
                trace(LOG, "Completed key [%s]: %s", key, sb.toString());
                repo.remove(key, sb.toString());
                repo.confirm(key);
            }
        }

        repo.getHawtDBFile().stop();
        traceEnd(LOG, "CamelTest.testCamel");
    }

    protected String key(int i) {
        return "key:" + i;
    }

}
