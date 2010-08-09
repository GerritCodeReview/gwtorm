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
import com.google.gwtorm.client.OrmDuplicateKeyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.nosql.CounterShard;
import com.google.gwtorm.nosql.IndexKeyBuilder;
import com.google.gwtorm.nosql.IndexRow;
import com.google.gwtorm.nosql.NoSqlSchema;
import com.google.gwtorm.schema.SequenceModel;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Base implementation for {@link Schema} in a {@link GenericDatabase}.
 * <p>
 * NoSQL implementors must extend this class and provide implementations for the
 * abstract methods declared here. Each schema instance will wrap one thread's
 * connection to the data store. Therefore, unlike database, this class does not
 * need to be thread-safe.
 */
public abstract class GenericSchema extends NoSqlSchema {
  private final GenericDatabase<?, ?, ?> db;

  protected GenericSchema(final GenericDatabase<?, ?, ?> d) {
    super(d);
    db = d;
  }

  /** @return the database that created this schema instance. */
  public GenericDatabase<?, ?, ?> getDatabase() {
    return db;
  }

  /**
   * Allocate a new unique value from a pool of values.
   * <p>
   * This method is only required to return a unique value per invocation.
   * Implementors may override the method to provide an implementation that
   * returns values out of order.
   * <p>
   * The default implementation of this method stores a {@link CounterShard}
   * under the row key {@code ".sequence." + poolName}, and updates it through
   * the atomic semantics of {@link #atomicUpdate(byte[], AtomicUpdate)}. If the
   * row does not yet exist, it is initialized and the value 1 is returned.
   *
   * @param poolName name of the value pool to allocate from. This is typically
   *        the name of a sequence in the schema.
   * @return a new unique value.
   * @throws OrmException a unique value cannot be obtained.
   */
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
            long start = 1;
            for (SequenceModel s : getDatabase().getSchemaModel()
                .getSequences()) {
              if (poolName.equals(s.getSequenceName())) {
                start = s.getSequence().startWith();
                if (start == 0) {
                  start = 1;
                }
                break;
              }
            }
            ctr = new CounterShard(start, Long.MAX_VALUE);
          }

          if (ctr.isEmpty()) {
            throw new NoMoreValues();
          }

          res[0] = ctr.next();
          return CounterShard.CODEC.encodeToByteString(ctr).toByteArray();
        }
      });
      return res[0];
    } catch (NoMoreValues err) {
      throw new OrmException("Counter '" + poolName + "' out of values");
    }
  }

  /**
   * Fetch one row's data.
   * <p>
   * The default implementation of this method creates a pair of keys and passes
   * them to {@link #scan(byte[], byte[], int, boolean)}. The {@code fromKey} is
   * the supplied {@code key}, while the {@code toKey} has '\0' appended onto
   * {@code key}. If more than one row matches in that range, the method throws
   * an exception.
   *
   * @param key key of the row to fetch and return.
   * @return the data stored under {@code key}; null if no row exists.
   * @throws OrmDuplicateKeyException more than one row was identified in the
   *         key scan.
   * @throws OrmException the data store cannot process the request.
   */
  public byte[] fetchRow(byte[] key) throws OrmDuplicateKeyException,
      OrmException {
    final byte[] fromKey = key;
    final byte[] toKey = new byte[key.length + 1];
    System.arraycopy(key, 0, toKey, 0, key.length);

    ResultSet<Entry<byte[], byte[]>> r = scan(fromKey, toKey, 2, false);
    try {
      Iterator<Entry<byte[], byte[]>> i = r.iterator();
      if (!i.hasNext()) {
        return null;
      }

      byte[] data = i.next().getValue();
      if (i.hasNext()) {
        throw new OrmDuplicateKeyException("Unexpected duplicate keys");
      }
      return data;
    } finally {
      r.close();
    }
  }

  /**
   * Scan a range of keys and return any matching objects.
   * <p>
   * To fetch a single record with a scan, set {@code toKey} to the same array
   * as {@code fromKey}, but append a trailing NUL byte (0x00). The caller
   * should validate that the returned ResultSet contains no more than 1 row.
   * <p>
   * The resulting iteration does not support remove.
   * <p>
   * Each iteration element is a map entry, describing the row key and the row
   * value. The map entry's value cannot be changed.
   *
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return.
   * @param order if true the order will be preserved, false if the result order
   *        order can be arbitrary.
   * @return result iteration for the requested range. The result set may be
   *         lazily filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  public abstract ResultSet<Map.Entry<byte[], byte[]>> scan(byte[] fromKey,
      byte[] toKey, int limit, boolean order) throws OrmException;

  /**
   * Atomically insert one row, failing if the row already exists.
   * <p>
   * The default implementation of this method relies upon the atomic nature of
   * the {@link #atomicUpdate(byte[], AtomicUpdate)} primitive to test for the
   * row's existence, and create the row only if it is not found.
   *
   * @param key key of the new row to insert.
   * @param newData data of the new row.
   * @throws OrmDuplicateKeyException another row already exists with the
   *         specified key.
   * @throws OrmException the data store cannot process the request right now,
   *         for example due to a network connectivity problem.
   */
  public void insert(byte[] key, final byte[] newData)
      throws OrmDuplicateKeyException, OrmException {
    try {
      atomicUpdate(key, new AtomicUpdate<byte[]>() {
        @Override
        public byte[] update(byte[] oldData) {
          if (oldData != null) {
            throw new KeyExists();
          }
          return newData;
        }
      });
    } catch (KeyExists err) {
      throw new OrmDuplicateKeyException("Duplicate key");
    }
  }

  /**
   * Update a single row, inserting it if it does not exist.
   * <p>
   * Unlike insert, this method always succeeds.
   *
   * @param key key of the row to update, or insert if missing.
   * @param data data to store at this row.
   * @throws OrmException the data store cannot process the request, for example
   *         due to a network connectivity problem.
   */
  public abstract void upsert(byte[] key, byte[] data) throws OrmException;

  /**
   * Delete the row stored under the given key.
   * <p>
   * If the row does not exist, this method must complete successfully anyway.
   * The intent of the caller is to ensure the row does not exist when the
   * method completes, and a row that did not exist satisfies that intent.
   *
   * @param key the key to delete.
   * @throws OrmException the data store cannot perform the removal.
   */
  public abstract void delete(byte[] key) throws OrmException;

  /**
   * Atomically read and update a single row.
   * <p>
   * Unlike schema's atomicUpdate() method, this method must handle missing
   * rows. Implementations must be logically equivalent to the following, but
   * performed atomically within the scope of the single row key:
   *
   * <pre>
   * byte[] oldData = get(key);
   * byte[] newData = update.update(oldData);
   * if (newData != null) {
   *   upsert(key, newData);
   * } else if (oldData != null) {
   *   remove(key);
   * }
   * return data;
   * </pre>
   * <p>
   * Secondary index row updates are assumed to never be part of the atomic
   * update transaction. This is an intentional design decision to fit with many
   * NoSQL product's limitations to support only single-row atomic updates.
   * <p>
   * The {@code update} method may be invoked multiple times before the
   * operation is considered successful. This permits an implementation to
   * perform an opportunistic update attempt, and retry the update if the same
   * row was modified by another concurrent worker.
   *
   * @param key the row key to read, update and return.
   * @param update action to perform on the row's data element. The action may
   *        be passed null if the row doesn't exist.
   * @throws OrmException the database cannot perform the update.
   */
  public abstract void atomicUpdate(byte[] key, AtomicUpdate<byte[]> update)
      throws OrmException;

  /**
   * Check (and delete) an index row if its a fossil.
   * <p>
   * As index rows are written ahead of the main data row being written out,
   * scans sometimes see an index row that does not match the data row. These
   * are ignored for a short period ({@link GenericDatabase#getMaxFossilAge()})
   * to allow the primary data row to eventually get written out. If however the
   * writer never finished the update, these index rows are stale and need to be
   * pruned. Any index row older than the fossil age is removed by this method.
   *
   * @param now timestamp when the current scan started.
   * @param key the index row key.
   * @param row the index row data.
   */
  public void maybeFossilCollectIndexRow(long now, byte[] key, IndexRow row) {
    if (row.getTimestamp() + db.getMaxFossilAge() <= now) {
      fossilCollectIndexRow(key, row);
    }
  }

  /**
   * Delete the given fossil index row.
   * <p>
   * This method is logically the same as {@link #delete(byte[])}, but its
   * separated out to permit asynchronous delivery of the delete events since
   * these are arriving during an index scan and are less time-critical than
   * other delete operations.
   * <p>
   * The default implementation of this method calls {@link #delete(byte[])}.
   *
   * @param key index key to remove.
   * @param row the index row data.
   */
  protected void fossilCollectIndexRow(byte[] key, IndexRow row) {
    try {
      delete(key);
    } catch (OrmException e) {
      // Ignore a fossil delete error.
    }
  }

  private static class KeyExists extends RuntimeException {
  }

  private static class NoMoreValues extends RuntimeException {
  }
}
