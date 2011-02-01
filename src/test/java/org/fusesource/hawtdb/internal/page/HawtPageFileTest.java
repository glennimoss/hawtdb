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

import java.io.File;

import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.io.MemoryMappedFile;
import org.fusesource.hawtdb.internal.page.HawtPageFile;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HawtPageFileTest {

    @Test
    public void testHawtPageFile() throws Exception {
        File file = new File("target/hawtPageFile.data");
        file.delete();

        MemoryMappedFile mmf = new MemoryMappedFile(file, 5);

        HawtPageFile hpf = new HawtPageFile(mmf, (short)4, 3, 10);

        byte[] expect = "hello world".getBytes("UTF-8");
        Buffer buf = new Buffer(expect);

        hpf.write(0, buf);

        Buffer found = new Buffer(11);

        hpf.read(0, found);

        Assert.assertArrayEquals(expect, found.data);

        mmf.sync();
        mmf.close();

    }


}
