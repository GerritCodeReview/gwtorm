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

package com.google.gwtorm.nosql.generic;

import com.google.gwtorm.client.AtomicUpdate;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.nosql.CounterShard;
import com.google.gwtorm.nosql.IndexKeyBuilder;
import com.google.gwtorm.nosql.IndexRow;
import com.google.gwtorm.nosql.NoSqlSchema;

import java.util.Map;

/** Base implementation for {@link Schema} in a {@link GenericDatabase}. */
public abstract class GenericSchema extends NoSqlSchema {
  private final GenericDatabase<?, ?, ?> db;

  protected GenericSchema(final GenericDatabase<?, ?, ?> d) {
    super(d);
    db = d;
  }

  public GenericDatabase<?, ?, ?> getDatabase() {
    return db;
  }

  @Override
  protected long nextLong(final String poolName) throws OrmException {
    IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(".sequence." + poolName);
    b.delimiter();
    try {
      final long[] res = new long[1];
      atomicUpdate(b.toByteArray(), new AtomicUpdate<byte[]>() {
        @Override
        public byte[] update(byte[] val) {
          CounterShard ctr;
          if (val != null) {
            ctr = CounterShard.CODEC.decode(val);
          } else {
            ctr = new CounterShard(1, Long.MAX_VALUE);
          }

          if (ctr.isEmpty()) {
            throw new NoMoreValues();
          }

          res[0] = ctr.next();
          return CounterShard.CODEC.encode(ctr).toByteArray();
        }
      });
      return res[0];
    } catch (NoMoreValues err) {
      throw new OrmException("Counter '" + poolName + "' out of values");
    }
  }

  public abstract byte[] get(byte[] key) throws OrmException;

  public abstract Iterable<Map.Entry<byte[], byte[]>> scan(byte[] fromKey,
      byte[] toKey) throws OrmException;

  public abstract void insert(byte[] key, byte[] data) throws OrmException;

  public void replace(byte[] key, byte[] data) throws OrmException {
    upsert(key, data);
  }

  public abstract void upsert(byte[] key, byte[] data) throws OrmException;

  public abstract void delete(byte[] key) throws OrmException;

  /**
   * Atomically read and update a single row.
   * <p>
   * Unlike the schema atomicUpdate, this method handles missing rows.
   * Implementations must be logically equivalent to the following, but
   * performed atomically within the scope of the single row key:
   *
   * <pre>
   * byte[] oldData = get(key);
   * byte[] newData = update.update(oldData);
   * if (newData != null)
   *   upsert(key, newData);
   * else if (oldData != null) remove(key);
   * return data;
   * </pre>
   *
   * @param key the row key to read, update and return.
   * @param update action to perform on the row's data element. May be passed in
   *        null if the row doesn't exist, and should return the new row data,
   *        or null to remove the row.
   * @return the return value of {@code update}.
   * @throws OrmException the database cannot perform the update.
   */
  public abstract byte[] atomicUpdate(byte[] key, AtomicUpdate<byte[]> update)
      throws OrmException;

  public void maybeFossilCollectIndexRow(long now, byte[] key, IndexRow r)
      throws OrmException {
    if (r.getTimestamp() + db.getMaxFossilAge() <= now) {
      delete(key);
    }
  }

  private static class NoMoreValues extends RuntimeException {
  }
}
