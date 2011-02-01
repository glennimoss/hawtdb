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

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.fusesource.hawtdb.api.PageFile;
import org.fusesource.hawtdb.api.PageFileFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentTest {

    private final static Log LOG = LogFactory.getLog(ExtentTest.class);

	private PageFileFactory pff;
    private PageFile paged;

    protected PageFileFactory createPageFileFactory() {
	    PageFileFactory rc = new PageFileFactory();
        rc.setPageSize((short)19);
	    rc.setMappingSegementSize(19);
	    rc.setFile(new File("target/test-data/"+getClass().getName()+".db"));
	    return rc;
	}

	@Before
	public void setUp() throws Exception {
        pff = createPageFileFactory();
        pff.getFile().delete();
        pff.open();
        paged = pff.getPageFile();
	}

	@After
	public void tearDown() throws Exception {
	    pff.close();
	}

	protected void reload() {
        pff.close();
        pff.open();
        paged = pff.getPageFile();
	}


    @Test
	public void testExtentStreams() throws IOException {
        traceStart(LOG, "ExtentTest.testExtentStreams()");
        ExtentOutputStream eos = new ExtentOutputStream(paged, (short)19);
        DataOutputStream os = new DataOutputStream(eos);
        for (int i = 0; i < 100; i++) {
            os.writeUTF(String.format("Test string:%02d", i));
        }
        os.close();
        int page = eos.getPage();

        assertEquals(0, page);

        // Reload the page file.
        reload();

        ExtentInputStream eis = new ExtentInputStream(paged, page);
        DataInputStream is = new DataInputStream(eis);
        for (int i = 0; i < 100; i++) {
            assertEquals(String.format("Test string:%02d", i), is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();
        traceEnd(LOG, "ExtentTest.testExtentStreams");
    }
}
