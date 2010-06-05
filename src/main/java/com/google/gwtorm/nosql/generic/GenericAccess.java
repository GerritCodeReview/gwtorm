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

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.AtomicUpdate;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmDuplicateKeyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.client.impl.ListResultSet;
import com.google.gwtorm.nosql.IndexFunction;
import com.google.gwtorm.nosql.IndexKeyBuilder;
import com.google.gwtorm.nosql.IndexRow;
import com.google.gwtorm.nosql.NoSqlAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/** Base implementation for {@link Access} in a {@link GenericDatabase}. */
public abstract class GenericAccess<T, K extends Key<?>> extends
    NoSqlAccess<T, K> {
  private final GenericSchema db;

  protected GenericAccess(final GenericSchema s) {
    super(s);
    db = s;
  }

  @Override
  protected ResultSet<T> scan(String indexName, byte[] fromKey, byte[] toKey,
      int limit) throws OrmException {
    if (indexName.equals(getKeyIndex().getName())) {
      return scanDataRow(fromKey, toKey, limit);

    } else {
      return scanIndexRow(indexName, fromKey, toKey, limit);
    }
  }

  /**
   * Lookup a single entity via its primary key.
   * <p>
   * The default implementation of this method performs a scan over the primary
   * key with {@link #scan(String, byte[], byte[], int)}, '\0' appended onto
   * the fromKey copy and a result limit of 2.
   * <p>
   * If multiple records are discovered {@link OrmDuplicateKeyException} is
   * thrown back to the caller.
   *
   * @param key the primary key instance; must not be null.
   * @return the entity; null if no entity has this key.
   * @throws OrmException the data lookup failed.
   * @throws OrmDuplicateKeyException more than one row matched in the scan.
   */
  @Override
  public T get(K key) throws OrmException, OrmDuplicateKeyException {
    final String primary = getKeyIndex().getName();

    final IndexKeyBuilder dst = new IndexKeyBuilder();
    encodeKey(dst, key);

    final byte[] fromKey = dst.toByteArray();

    dst.nul();
    final byte[] toKey = dst.toByteArray();

    Iterator<T> r = scan(primary, fromKey, toKey, 2).iterator();
    if (!r.hasNext()) {
      return null;
    }

    T obj = r.next();
    if (r.hasNext()) {
      throw new OrmDuplicateKeyException("Duplicate " + getRelationName());
    }
    return obj;
  }

  private ResultSet<T> scanDataRow(byte[] fromKey, byte[] toKey, int limit)
      throws OrmException {
    IndexKeyBuilder b;

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    b.addRaw(fromKey);
    fromKey = b.toByteArray();

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    b.addRaw(toKey);
    toKey = b.toByteArray();

    final ArrayList<T> res = new ArrayList<T>();
    for (Map.Entry<byte[], byte[]> ent : db.scan(fromKey, toKey)) {
      res.add(getObjectCodec().decode(ent.getValue()));
      if (limit > 0 && res.size() == limit) {
        break;
      }
    }
    return new ListResultSet<T>(res);
  }

  private ResultSet<T> scanIndexRow(String indexName, byte[] fromKey,
      byte[] toKey, int limit) throws OrmException {
    final long now = System.currentTimeMillis();
    IndexKeyBuilder b;

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(indexName);
    b.delimiter();
    b.addRaw(fromKey);
    fromKey = b.toByteArray();

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(indexName);
    b.delimiter();
    b.addRaw(toKey);
    toKey = b.toByteArray();

    final IndexFunction<T> idx = getQueryIndex(indexName);
    final ArrayList<T> res = new ArrayList<T>();
    for (Map.Entry<byte[], byte[]> ent : db.scan(fromKey, toKey)) {

      // Decode the row and try to get the object data. If its
      // not stored in this row in the secondary index we need
      // to get the authoritative copy from the main index.
      //
      final IndexRow r = IndexRow.CODEC.decode(ent.getValue());
      byte[] objData = r.getDataCopy();
      if (objData == null) {
        b = new IndexKeyBuilder();
        b.add(getRelationName());
        b.delimiter();
        b.addRaw(r.getDataKey());
        objData = db.get(b.toByteArray());
      }

      // If we have no data present and this row is stale enough,
      // drop the row out of the index.
      //
      final byte[] idxkey = ent.getKey();
      if (objData == null) {
        db.maybeFossilCollectIndexRow(now, idxkey, r);
        continue;
      }

      // Verify the object still matches the predicate of the index.
      // If it does, include it in the result. Otherwise, maybe we
      // should drop it from the index.
      //
      final T obj = getObjectCodec().decode(objData);
      if (matches(idx, obj, idxkey)) {
        res.add(obj);
        if (limit > 0 && res.size() == limit) {
          break;
        }
      } else {
        db.maybeFossilCollectIndexRow(now, idxkey, r);
      }
    }
    return new ListResultSet<T>(res);
  }

  @Override
  public void insert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      insertOne(obj);
    }
  }

  private void insertOne(T obj) throws OrmException {
    byte[] idx = indexRowData(obj);

    for (IndexFunction<T> f : getQueryIndexes()) {
      if (f.includes(obj)) {
        db.upsert(indexRowKey(f, obj), idx);
      }
    }

    db.insert(dataRowKey(obj), getObjectCodec().encode(obj).toByteArray());
  }

  @Override
  public void update(Iterable<T> instances) throws OrmException {
    upsert(instances);
  }

  @Override
  public void upsert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      upsertOne(obj);
    }
  }

  private void upsertOne(T newObj) throws OrmException {
    final byte[] key = dataRowKey(newObj);
    final byte[] oldBin = db.get(key);
    final T oldObj = oldBin != null ? getObjectCodec().decode(oldBin) : null;

    writeNewIndexes(oldObj, newObj);
    db.upsert(key, getObjectCodec().encode(newObj).toByteArray());
    pruneOldIndexes(oldObj, newObj);
  }

  private void writeNewIndexes(T oldObj, final T newObj) throws OrmException {
    final byte[] idx = indexRowData(newObj);

    // Write any secondary index records first if they differ
    // from what would already be there for the prior version.
    //
    for (IndexFunction<T> f : getQueryIndexes()) {
      if (f.includes(newObj)) {
        final byte[] row = indexRowKey(f, newObj);
        if (oldObj == null || !matches(f, oldObj, row)) {
          db.upsert(row, idx);
        }
      }
    }
  }

  private void pruneOldIndexes(final T oldObj, T newObj) throws OrmException {
    // Prune any old index records which no longer match.
    //
    if (oldObj != null) {
      for (IndexFunction<T> f : getQueryIndexes()) {
        if (f.includes(oldObj)) {
          final byte[] k = indexRowKey(f, oldObj);
          if (!matches(f, newObj, k)) {
            db.delete(k);
          }
        }
      }
    }
  }

  @Override
  public void delete(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      db.delete(dataRowKey(obj));

      for (IndexFunction<T> f : getQueryIndexes()) {
        if (f.includes(obj)) {
          db.delete(indexRowKey(f, obj));
        }
      }
    }
  }

  @Override
  public T atomicUpdate(K key, final AtomicUpdate<T> update)
      throws OrmException {
    final IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    encodeKey(b, key);

    try {
      final T[] res = (T[]) new Object[3];
      db.atomicUpdate(b.toByteArray(), new AtomicUpdate<byte[]>() {
        @Override
        public byte[] update(byte[] data) {
          if (data != null) {
            final T oldObj = getObjectCodec().decode(data);
            final T newObj = getObjectCodec().decode(data);
            res[0] = update.update(newObj);
            res[1] = oldObj;
            res[2] = newObj;
            try {
              writeNewIndexes(oldObj, newObj);
            } catch (OrmException err) {
              throw new IndexException(err);
            }
            return getObjectCodec().encode(newObj).toByteArray();

          } else {
            res[0] = null;
            return null;
          }
        }
      });
      if (res[0] != null) {
        pruneOldIndexes(res[1], res[2]);
      }
      return res[0];
    } catch (IndexException err) {
      throw err.cause;
    }
  }

  private boolean matches(IndexFunction<T> f, T obj, byte[] exp) {
    return f.includes(obj) && Arrays.equals(exp, indexRowKey(f, obj));
  }

  private byte[] dataRowKey(T obj) {
    IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    getKeyIndex().encode(b, obj);
    return b.toByteArray();
  }

  private byte[] indexRowKey(IndexFunction<T> f, T obj) {
    IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(f.getName());
    b.delimiter();
    f.encode(b, obj);
    b.delimiter();
    getKeyIndex().encode(b, obj);
    return b.toByteArray();
  }

  private byte[] indexRowData(T obj) {
    final long now = System.currentTimeMillis();

    final IndexKeyBuilder b = new IndexKeyBuilder();
    getKeyIndex().encode(b, obj);
    final byte[] key = b.toByteArray();

    return IndexRow.CODEC.encode(IndexRow.forKey(now, key)).toByteArray();
  }

  private static class IndexException extends RuntimeException {
    final OrmException cause;

    IndexException(OrmException err) {
      super(err);
      this.cause = err;
    }
  }
}
