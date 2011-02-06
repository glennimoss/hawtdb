/**
 *  Copyright (C) 2009, Progress Software Corporation and/or its
 * subsidiaries or affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.internal.page;

import org.apache.commons.logging.Log;

import java.util.HashSet;

import org.fusesource.hawtbuf.Buffer;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Tracer {

    private static String indent = "";

    public static void traceStart(Log log, String message, Object...args) {
      if (log.isTraceEnabled() ) {
        trace(log, message, args);
        indent = indent + ". ";
      }
    }

    public static void trace(Log log, String message, Object...args) {
        if( log.isTraceEnabled() ) {
            log.trace(indent + String.format(message, args));
        }
    }

    public static void traceEnd(Log log, String message, Object...args) {
      if (log.isTraceEnabled() ) {
        indent = indent.substring(2);
        trace(log, message, args);
      }
    }

    static public String buf(Buffer value) {
        return "{ offset: "+value.offset+", length: "+value.length+", data: "+ hexify(value) + " }";
    }

    static public String hexify(Buffer value) {
        int size = value.getLength();
        StringBuilder sb = new StringBuilder(size * 4);
        sb.append('[');
        String sep = "";
        for( int i=0; i < size; i++ ) {
          sb.append(String.format("%s%02X", sep, value.get(i)));
          sep = ", ";
        }
        sb.append(']');
        return sb.toString();
    }

    static public String hexify(byte[] data) {
      return hexify(new Buffer(data));
    }

}
