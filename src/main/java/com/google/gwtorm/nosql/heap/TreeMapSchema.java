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

import com.google.gwtorm.nosql.generic.GenericSchema;
import com.google.gwtorm.nosql.generic.Row;
import com.google.gwtorm.server.AtomicUpdate;
import com.google.gwtorm.server.ListResultSet;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.ResultSet;
import com.google.gwtorm.server.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/** Base implementation for {@link Schema} in a {@link TreeMapDatabase}. */
public abstract class TreeMapSchema extends GenericSchema {
  private final TreeMapDatabase<?, ?, ?> db;

  protected TreeMapSchema(final TreeMapDatabase<?, ?, ?> d) {
    super(d);
    db = d;
  }

  @Override
  public void flush() {
    // We don't buffer writes.
  }

  @Override
  public void close() {
    // Nothing to do.
  }

  @Override
  public ResultSet<Row> scan(byte[] fromKey, byte[] toKey, int limit, boolean order) {
    db.lock.lock();
    try {
      final List<Row> res = new ArrayList<>();
      for (Map.Entry<byte[], byte[]> ent : entries(fromKey, toKey)) {
        res.add(new Row(ent.getKey(), ent.getValue()));

        if (limit > 0 && res.size() == limit) {
          break;
        }
      }
      return new ListResultSet<>(res);
    } finally {
      db.lock.unlock();
    }
  }

  private Set<Entry<byte[], byte[]>> entries(byte[] fromKey, byte[] toKey) {
    return db.table.subMap(fromKey, toKey).entrySet();
  }

  @Override
  public void upsert(byte[] key, byte[] data) throws OrmException {
    db.lock.lock();
    try {
      db.table.put(key, data);
    } finally {
      db.lock.unlock();
    }
  }

  @Override
  public void delete(byte[] key) throws OrmException {
    db.lock.lock();
    try {
      db.table.remove(key);
    } finally {
      db.lock.unlock();
    }
  }

  @Override
  public void atomicUpdate(byte[] key, AtomicUpdate<byte[]> update) throws OrmException {
    db.lock.lock();
    try {
      final byte[] oldData = fetchRow(key);
      final byte[] newData = update.update(oldData);
      if (newData != null) {
        upsert(key, newData);
      } else if (oldData != null) {
        delete(key);
      }
    } finally {
      db.lock.unlock();
    }
  }
}
