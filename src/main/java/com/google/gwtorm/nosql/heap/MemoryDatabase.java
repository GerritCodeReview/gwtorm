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

import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;

/**
 * Toy in-memory implementation of a NoSQL database.
 * <p>
 * Implements a simple NoSQL database with a standard {@link java.util.TreeMap}
 * held inside of this JVM process. All operations occur on the TreeMap, with no
 * durability across database restarts. Therefore this implementation is only
 * suitable for simple tests.
 *
 * @param <T> type of the application schema.
 * @see FileDatabase
 */
@SuppressWarnings("rawtypes")
public class MemoryDatabase<T extends Schema> extends
    TreeMapDatabase<T, TreeMapSchema, TreeMapAccess> {

  /**
   * Create the database and implement the application's schema interface.
   *
   * @param schema the application schema this database will open.
   * @throws OrmException the schema cannot be queried.
   */
  public MemoryDatabase(final Class<T> schema) throws OrmException {
    super(TreeMapSchema.class, TreeMapAccess.class, schema);
  }
}
