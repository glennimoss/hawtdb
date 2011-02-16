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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdb.api.SortedIndex;
import org.fusesource.hawtdb.api.Transaction;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.fusesource.hawtdb.internal.page.Tracer.*;

/**
 * An instance of AggregationRepository which is backed by a HawtDB.
 */
public class Repository {

    private static final Log LOG = LogFactory.getLog(Repository.class);

    private HawtDBFile hawtDBFile;
    private String repositoryName;
    private int bufferSize = 8 * 1024 * 1024;
    private boolean sync = true;
    private short pageSize = 512;
    private boolean returnOldExchange;
    private StringCodec codec = StringCodec.INSTANCE;
    private long recoveryInterval = 5000;
    private boolean useRecovery = true;
    private int maximumRedeliveries;
    private String deadLetterUri;

    public Repository(String repositoryName, File persistentFile) {
        this.repositoryName = repositoryName;

        hawtDBFile = new HawtDBFile();
        hawtDBFile.setFile(persistentFile);
        hawtDBFile.setSync(isSync());
        if (getBufferSize() != null) {
            hawtDBFile.setMappingSegementSize(getBufferSize());
        }
        if (getPageSize() > 0) {
            hawtDBFile.setPageSize(getPageSize());
        }

        hawtDBFile.start();
    }

    private Buffer marshal (String str) throws IOException {
        DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
        codec.encode(str, baos);
        return baos.toBuffer();
    }

    public String unmarshal (Buffer buffer) throws IOException {
        DataByteArrayInputStream bais = new DataByteArrayInputStream(buffer);
        String key = codec.decode(bais);
        return key;
    }

    public String add(final String key, final String value) {
        try {
            // If we could guarantee that the key and exchange are immutable,
            // then we could have stuck them directly into the index,
            // HawtDB could then eliminate the need to marshal and un-marshal
            // in some cases.  But since we can't.. we are going to force
            // early marshaling.
            final Buffer keyBuffer = marshal(key);
            final Buffer valueBuffer = marshal(value);
            Buffer rc = hawtDBFile.execute(new Work<Buffer>() {
                public Buffer execute(Transaction tx) {
                    SortedIndex<Buffer, Buffer> index = hawtDBFile.getRepositoryIndex(tx, repositoryName, true);
                    return index.put(keyBuffer, valueBuffer);
                }

                @Override
                public String toString() {
                    return "Adding key [" + key + "]";
                }
            });
            if (rc == null) {
                return null;
            }

            // only return old exchange if enabled
            if (isReturnOldExchange()) {
                return unmarshal(rc);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error adding to repository " + repositoryName + " with key " + key, e);
        }

        return null;
    }

    public String get(final String key) {
        String answer = null;
        try {
            final Buffer keyBuffer = marshal(key);
            Buffer rc = hawtDBFile.execute(new Work<Buffer>() {
                public Buffer execute(Transaction tx) {
                    SortedIndex<Buffer, Buffer> index = hawtDBFile.getRepositoryIndex(tx, repositoryName, false);
                    if (index == null) {
                        return null;
                    }
                    return index.get(keyBuffer);
                }

                @Override
                public String toString() {
                    return "Getting key [" + key + "]";
                }
            });
            if (rc != null) {
                answer = unmarshal(rc);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error getting key " + key + " from repository " + repositoryName, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting key  [" + key + "] -> " + answer);
        }
        return answer;
    }

    public void remove(final String key, final String value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing key [" + key + "]");
        }
        try {
            final Buffer keyBuffer = marshal(key);
            final Buffer confirmKeyBuffer = marshal(key);
            final Buffer valueBuffer = marshal(value);
            hawtDBFile.execute(new Work<Buffer>() {
                public Buffer execute(Transaction tx) {
                    SortedIndex<Buffer, Buffer> index = hawtDBFile.getRepositoryIndex(tx, repositoryName, true);
                    // remove from the in progress index
                    index.remove(keyBuffer);

                    // and add it to the confirmed index
                    SortedIndex<Buffer, Buffer> indexCompleted = hawtDBFile.getRepositoryIndex(tx, getRepositoryNameCompleted(), true);
                    indexCompleted.put(confirmKeyBuffer, valueBuffer);
                    return null;
                }

                @Override
                public String toString() {
                    return "Removing key [" + key + "]";
                }
            });

        } catch (IOException e) {
            throw new RuntimeException("Error removing key " + key + " from repository " + repositoryName, e);
        }
    }

    public void confirm(final String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Confirming key [" + key + "]");
        }
        try {
            final Buffer confirmKeyBuffer = marshal(key);
            hawtDBFile.execute(new Work<Buffer>() {
                public Buffer execute(Transaction tx) {
                    SortedIndex<Buffer, Buffer> indexCompleted = hawtDBFile.getRepositoryIndex(tx, getRepositoryNameCompleted(), true);
                    return indexCompleted.remove(confirmKeyBuffer);
                }

                @Override
                public String toString() {
                    return "Confirming key [" + key + "]";
                }
            });

        } catch (IOException e) {
            throw new RuntimeException("Error confirming key " + key + " from repository " + repositoryName, e);
        }
    }

    public Set<String> getKeys() {
        final Set<String> keys = new LinkedHashSet<String>();

        hawtDBFile.execute(new Work<Buffer>() {
            public Buffer execute(Transaction tx) {

                SortedIndex<Buffer, Buffer> index = hawtDBFile.getRepositoryIndex(tx, repositoryName, false);
                if (index == null) {
                    return null;
                }

                Iterator<Map.Entry<Buffer, Buffer>> it = index.iterator();
                // scan could potentially be running while we are shutting down so check for that
                while (it.hasNext()) {
                    Map.Entry<Buffer, Buffer> entry = it.next();
                    Buffer keyBuffer = entry.getKey();

                    String key;
                    try {
                        key  = unmarshal(keyBuffer);
                    } catch (IOException e) {
                        throw new RuntimeException("Error unmarshalling key: " + keyBuffer, e);
                    }
                    if (key != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("getKey [" + key + "]");
                        }
                        keys.add(key);
                    }
                }
                return null;

            }

            @Override
            public String toString() {
                return "getKeys";
            }
        });

        return Collections.unmodifiableSet(keys);
    }

    /* imma ignore this guy
    public Set<String> scan(CamelContext camelContext) {
        final Set<String> answer = new LinkedHashSet<String>();
        hawtDBFile.execute(new Work<Buffer>() {
            public Buffer execute(Transaction tx) {
                // scan could potentially be running while we are shutting down so check for that
                if (!isRunAllowed()) {
                    return null;
                }

                SortedIndex<Buffer, Buffer> indexCompleted = hawtDBFile.getRepositoryIndex(tx, getRepositoryNameCompleted(), false);
                if (indexCompleted == null) {
                    return null;
                }

                Iterator<Map.Entry<Buffer, Buffer>> it = indexCompleted.iterator();
                // scan could potentially be running while we are shutting down so check for that
                while (it.hasNext() && isRunAllowed()) {
                    Map.Entry<Buffer, Buffer> entry = it.next();
                    Buffer keyBuffer = entry.getKey();

                    String exchangeId;
                    try {
                        exchangeId = codec.unmarshallKey(keyBuffer);
                    } catch (IOException e) {
                        throw new RuntimeException("Error unmarshalling confirm key: " + keyBuffer, e);
                    }
                    if (exchangeId != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Scan exchangeId [" + exchangeId + "]");
                        }
                        answer.add(exchangeId);
                    }
                }
                return null;
            }

            @Override
            public String toString() {
                return "Scan";
            }
        });

        if (answer.size() == 0) {
            LOG.trace("Scanned and found no exchange to recover.");
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Scanned and found " + answer.size() + " exchange(s) to recover (note some of them may already be in progress).");
            }
        }
        return answer;

    }
    */

    /* also hopefully not relevant to the test
    public Exchange recover(CamelContext camelContext, final String exchangeId) {
        Exchange answer = null;
        try {
            final Buffer confirmKeyBuffer = codec.marshallKey(exchangeId);
            Buffer rc = hawtDBFile.execute(new Work<Buffer>() {
                public Buffer execute(Transaction tx) {
                    SortedIndex<Buffer, Buffer> indexCompleted = hawtDBFile.getRepositoryIndex(tx, getRepositoryNameCompleted(), false);
                    if (indexCompleted == null) {
                        return null;
                    }
                    return indexCompleted.get(confirmKeyBuffer);
                }

                @Override
                public String toString() {
                    return "Recovering exchangeId [" + exchangeId + "]";
                }
            });
            if (rc != null) {
                answer = codec.unmarshallExchange(camelContext, rc);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error recovering exchangeId " + exchangeId + " from repository " + repositoryName, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering exchangeId [" + exchangeId + "] -> " + answer);
        }
        return answer;
    }
    */

    /*
    private int size(final String repositoryName) {
        int answer = hawtDBFile.execute(new Work<Integer>() {
            public Integer execute(Transaction tx) {
                SortedIndex<Buffer, Buffer> index = hawtDBFile.getRepositoryIndex(tx, repositoryName, false);
                return index != null ? index.size() : 0;
            }

            @Override
            public String toString() {
                return "Size[" + repositoryName + "]";
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Size of repository [" + repositoryName + "] -> " + answer);
        }
        return answer;
    }
    */

    public HawtDBFile getHawtDBFile() {
        return hawtDBFile;
    }

    public void setHawtDBFile(HawtDBFile hawtDBFile) {
        this.hawtDBFile = hawtDBFile;
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    private String getRepositoryNameCompleted() {
        return repositoryName + "-completed";
    }

    public void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isReturnOldExchange() {
        return returnOldExchange;
    }

    public void setReturnOldExchange(boolean returnOldExchange) {
        this.returnOldExchange = returnOldExchange;
    }

    public void setRecoveryInterval(long interval, TimeUnit timeUnit) {
        this.recoveryInterval = timeUnit.toMillis(interval);
    }

    public void setRecoveryInterval(long interval) {
        this.recoveryInterval = interval;
    }

    public long getRecoveryIntervalInMillis() {
        return recoveryInterval;
    }

    public boolean isUseRecovery() {
        return useRecovery;
    }

    public void setUseRecovery(boolean useRecovery) {
        this.useRecovery = useRecovery;
    }

    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    public String getDeadLetterUri() {
        return deadLetterUri;
    }

    public void setDeadLetterUri(String deadLetterUri) {
        this.deadLetterUri = deadLetterUri;
    }

    public short getPageSize() {
        return pageSize;
    }

    public void setPageSize(short pageSize) {
        this.pageSize = pageSize;
    }

}
