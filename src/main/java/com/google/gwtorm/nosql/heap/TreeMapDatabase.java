// Copyright 2010 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.gwtorm.nosql.heap;

import com.google.gwtorm.nosql.generic.GenericDatabase;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import java.io.PrintWriter;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Toy in-memory implementation of a NoSQL database.
 *
 * <p>Implements a simple NoSQL database with a standard {@link java.util.TreeMap} held inside of
 * this JVM process. All operations occur on the TreeMap, with no durability across database
 * restarts. Therefore this implementation is only suitable for simple tests.
 *
 * @param <T> type of the application schema.
 */
@SuppressWarnings("rawtypes")
abstract class TreeMapDatabase<T extends Schema, S extends TreeMapSchema, A extends TreeMapAccess>
    extends GenericDatabase<T, S, A> {

  /** Lock that protects reads and writes of {@link #table}. */
  final Lock lock;

  /** The NoSQL database storage. */
  final SortedMap<byte[], byte[]> table;

  /**
   * Initialize a new database and generate the implementation.
   *
   * @param schemaBaseType class that the generated Schema implementation should extend in order to
   *     provide data store connectivity.
   * @param accessBaseType class that the generated Access implementations should extend in order to
   *     provide single-relation access for each schema instance.
   * @param appSchema the application schema interface that must be implemented and constructed on
   *     demand.
   * @throws OrmException the schema cannot be created because of an annotation error in the
   *     interface definitions.
   */
  protected TreeMapDatabase(
      final Class<S> schemaBaseType, final Class<A> accessBaseType, final Class<T> appSchema)
      throws OrmException {
    super(schemaBaseType, accessBaseType, appSchema);

    lock = new ReentrantLock(true);
    table = new TreeMap<>(HeapKeyComparator.INSTANCE);
  }

  /**
   * Try to print the database contents in human readable format.
   *
   * @param pw writer to print the database out to.
   */
  public void dump(PrintWriter pw) {
    lock.lock();
    try {
      for (Map.Entry<byte[], byte[]> ent : table.entrySet()) {
        String key = format(ent.getKey());

        String val;
        try {
          UnknownFieldSet proto = UnknownFieldSet.parseFrom(ent.getValue());
          val = proto.toString();
        } catch (InvalidProtocolBufferException notProto) {
          val = format(ent.getValue());
        }

        if (val.contains("\n")) {
          pw.println(key + ":\n" + "  " + val.replaceAll("\n", "\n  "));
        } else {
          pw.println(key + ": " + val);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private static String format(byte[] bin) {
    StringBuilder s = new StringBuilder(bin.length);
    for (int i = 0; i < bin.length; i++) {
      byte b = bin[i];
      switch (b) {
        case 0x00:
          s.append("\\0");
          break;

        case 0x01:
          s.append("\\1");
          break;

        case -1:
          s.append("\\xff");
          break;

        case '\r':
          s.append("\\r");
          break;

        default:
          s.append((char) b);
          break;
      }
    }
    return s.toString();
  }
}
