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
import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmDuplicateKeyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.client.impl.AbstractResultSet;
import com.google.gwtorm.client.impl.ListResultSet;
import com.google.gwtorm.nosql.IndexFunction;
import com.google.gwtorm.nosql.IndexKeyBuilder;
import com.google.gwtorm.nosql.IndexRow;
import com.google.gwtorm.nosql.NoSqlAccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** Base implementation for {@link Access} in a {@link GenericDatabase}. */
public abstract class GenericAccess<T, K extends Key<?>> extends
    NoSqlAccess<T, K> {
  /** Maximum number of results to cache to improve updates on upsert. */
  private static final int MAX_SZ = 64;

  private final GenericSchema db;
  private LinkedHashMap<K, byte[]> cache;

  protected GenericAccess(final GenericSchema s) {
    super(s);
    db = s;
  }

  protected LinkedHashMap<K, byte[]> cache() {
    if (cache == null) {
      cache = new LinkedHashMap<K, byte[]>(8) {
        @Override
        protected boolean removeEldestEntry(Entry<K, byte[]> entry) {
          return MAX_SZ <= size();
        }
      };
    }
    return cache;
  }

  /**
   * Lookup a single entity via its primary key.
   * <p>
   * The default implementation of this method performs a scan over the primary
   * key with {@link #scanPrimaryKey(byte[], byte[], int)}, '\0' appended onto
   * the fromKey and a result limit of 2.
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
    final IndexKeyBuilder dst = new IndexKeyBuilder();
    encodePrimaryKey(dst, key);

    final byte[] fromKey = dst.toByteArray();

    dst.nul();
    final byte[] toKey = dst.toByteArray();

    Iterator<T> r = scanPrimaryKey(fromKey, toKey, 2).iterator();
    if (!r.hasNext()) {
      return null;
    }

    T obj = r.next();
    if (r.hasNext()) {
      throw new OrmDuplicateKeyException("Duplicate " + getRelationName());
    }
    return obj;
  }

  /**
   * Scan a range of keys from the data rows and return any matching objects.
   *
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  @Override
  protected ResultSet<T> scanPrimaryKey(byte[] fromKey, byte[] toKey, int limit)
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

    final ResultSet<Map.Entry<byte[], byte[]>> rs = db.scan(fromKey, toKey, limit);
    final Iterator<Map.Entry<byte[], byte[]>> i = rs.iterator();

    return new AbstractResultSet<T>() {
      @Override
      protected boolean hasNext() {
        return i.hasNext();
      }

      @Override
      protected T next() {
        byte[] bin = i.next().getValue();
        T obj = getObjectCodec().decode(bin);
        cache().put(primaryKey(obj), bin);
        return obj;
      }

      @Override
      public void close() {
        rs.close();
      }
    };
  }

  /**
   * Scan a range of index keys and return any matching objects.
   *
   * @param idx the index function describing the index to scan.
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  @Override
  protected ResultSet<T> scanIndex(IndexFunction<T> idx, byte[] fromKey,
      byte[] toKey, int limit) throws OrmException {
    final long now = System.currentTimeMillis();
    IndexKeyBuilder b;

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(idx.getName());
    b.delimiter();
    b.addRaw(fromKey);
    fromKey = b.toByteArray();

    b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(idx.getName());
    b.delimiter();
    b.addRaw(toKey);
    toKey = b.toByteArray();

    final ArrayList<T> res = new ArrayList<T>();
    byte[] lastKey = fromKey;

    SCAN: for (;;) {
      int scanned = 0;
      ResultSet<Entry<byte[], byte[]>> rs = db.scan(lastKey, toKey, limit);
      for (Map.Entry<byte[], byte[]> ent : rs) {
        final byte[] idxkey = ent.getKey();
        lastKey = idxkey;
        scanned++;

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
          objData = db.fetchRow(b.toByteArray());
        }

        // If we have no data present and this row is stale enough,
        // drop the row out of the index.
        //
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
          cache().put(primaryKey(obj), objData);
          res.add(obj);
          if (limit > 0 && res.size() == limit) {
            rs.close();
            break SCAN;
          }
        } else {
          db.maybeFossilCollectIndexRow(now, idxkey, r);
        }
      }

      // If we have no limit we scanned everything, so break out.
      // If scanned < limit, we saw every index row that might be
      // a match, and no further rows would exist.
      //
      if (limit == 0 || scanned < limit) {
        rs.close();
        break SCAN;
      }

      // Otherwise we have to scan again starting after lastKey.
      //
      b = new IndexKeyBuilder();
      b.addRaw(lastKey);
      b.nul();
      lastKey = b.toByteArray();
    }

    return new ListResultSet<T>(res);
  }

  private void maybeFlush() throws OrmException {
    if (db.isAutoFlush()) {
      db.flush();
    }
  }

  @Override
  public void insert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      insertOne(obj);
    }
    maybeFlush();
  }

  private void insertOne(T nObj) throws OrmException {
    writeNewIndexes(null, nObj);

    final byte[] key = dataRowKey(primaryKey(nObj));
    db.insert(key, getObjectCodec().encodeToByteString(nObj).toByteArray());
  }

  @Override
  public void update(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      upsertOne(obj, true);
    }
    maybeFlush();
  }

  @Override
  public void upsert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      upsertOne(obj, false);
    }
    maybeFlush();
  }

  private void upsertOne(T newObj, boolean mustExist) throws OrmException {
    final byte[] key = dataRowKey(primaryKey(newObj));

    T oldObj;
    byte[] oldBin = cache().get(primaryKey(newObj));
    if (oldBin != null) {
      oldObj = getObjectCodec().decode(oldBin);
    } else if (mustExist) {
      oldBin = db.fetchRow(key);
      if (oldBin != null) {
        oldObj = getObjectCodec().decode(oldBin);
      } else {
        throw new OrmConcurrencyException();
      }
    } else {
      oldObj = null;
    }

    writeNewIndexes(oldObj, newObj);
    db.upsert(key, getObjectCodec().encodeToByteString(newObj).toByteArray());
    pruneOldIndexes(oldObj, newObj);
  }

  /**
   * Insert secondary index rows for an object about to be written.
   * <p>
   * Insert or update operations should invoke this method before the main data
   * row is written, allowing the secondary index rows to be put into the data
   * store before the main data row arrives. Compatible scan implementations
   * (such as {@link #scanIndex(IndexFunction, byte[], byte[], int)} above) will
   * ignore these rows for a short time period.
   *
   * @param oldObj an old copy of the object; if non-null this may be used to
   *        avoid writing unnecessary secondary index rows that already exist.
   * @param newObj the new (or updated) object being stored. Must not be null.
   * @throws OrmException the data store is unable to update an index row.
   */
  protected void writeNewIndexes(T oldObj, T newObj) throws OrmException {
    final byte[] idxData = indexRowData(newObj);
    for (IndexFunction<T> f : getIndexes()) {
      if (f.includes(newObj)) {
        final byte[] idxKey = indexRowKey(f, newObj);
        if (oldObj == null || !matches(f, oldObj, idxKey)) {
          db.upsert(idxKey, idxData);
        }
      }
    }
  }

  /**
   * Remove old secondary index rows that are no longer valid for an object.
   *
   * @param oldObj an old copy of the object, prior to the current update taking
   *        place. If null the method does nothing and simply returns.
   * @param newObj the new copy of the object. Index rows that are still valid
   *        for {@code #newObj} are left alone. If null, all index rows for
   *        {@code oldObj} are removed.
   * @throws OrmException the data store is unable to remove an index row.
   */
  protected void pruneOldIndexes(final T oldObj, T newObj) throws OrmException {
    if (oldObj != null) {
      for (IndexFunction<T> f : getIndexes()) {
        if (f.includes(oldObj)) {
          final byte[] idxKey = indexRowKey(f, oldObj);
          if (newObj == null || !matches(f, newObj, idxKey)) {
            db.delete(idxKey);
          }
        }
      }
    }
  }

  @Override
  public void delete(Iterable<T> instances) throws OrmException {
    for (T oldObj : instances) {
      db.delete(dataRowKey(primaryKey(oldObj)));
      pruneOldIndexes(oldObj, null);
      cache().remove(primaryKey(oldObj));
    }
    maybeFlush();
  }

  @Override
  public T atomicUpdate(K key, final AtomicUpdate<T> update)
      throws OrmException {
    final IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    encodePrimaryKey(b, key);

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
            return getObjectCodec().encodeToByteString(newObj).toByteArray();

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

  /**
   * Determine if an object still matches the index row.
   * <p>
   * This method checks that the object's fields still match the criteria
   * necessary for it to be part of the index defined by {@code f}. It also
   * formats the index key and validates it is still identical to {@code exp}.
   *
   * @param f the function that defines the index.
   * @param obj the object instance being tested; must not be null.
   * @param exp the index row key, as scanned from the index.
   * @return true if the object still matches the data encoded in {@code #exp}.
   */
  protected boolean matches(IndexFunction<T> f, T obj, byte[] exp) {
    return f.includes(obj) && Arrays.equals(exp, indexRowKey(f, obj));
  }

  /**
   * Generate the row key for the object's primary data row.
   * <p>
   * The default implementation uses the relation name, a delimiter, and then
   * the encoded primary key.
   *
   * @param key key of the object.
   * @return the object's data row key.
   */
  protected byte[] dataRowKey(K key) {
    IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.delimiter();
    encodePrimaryKey(b, key);
    return b.toByteArray();
  }

  /**
   * Generate the row key for an object's secondary index row.
   * <p>
   * The default implementation uses the relation name, '.', the index name, a
   * delimiter, the indexed fields encoded, a delimiter, and then the encoded
   * primary key (without the relation name prefix).
   * <p>
   * The object's primary key is always appended onto the end of the secondary
   * index row key to ensure that objects with the same field values still get
   * distinct rows in the secondary index.
   *
   * @param idx function that describes the index.
   * @param obj the object the index record should reference.
   * @return the encoded secondary index row key.
   */
  protected byte[] indexRowKey(IndexFunction<T> idx, T obj) {
    IndexKeyBuilder b = new IndexKeyBuilder();
    b.add(getRelationName());
    b.add('.');
    b.add(idx.getName());
    b.delimiter();
    idx.encode(b, obj);
    b.delimiter();
    encodePrimaryKey(b, primaryKey(obj));
    return b.toByteArray();
  }

  /**
   * Generate the data to store in a secondary index row for an object.
   * <p>
   * The default implementation of this method stores the encoded primary key,
   * and the current system timestamp.
   *
   * @param obj the object the index record should reference.
   * @return the encoded secondary index row data.
   */
  protected byte[] indexRowData(T obj) {
    final long now = System.currentTimeMillis();

    final IndexKeyBuilder b = new IndexKeyBuilder();
    encodePrimaryKey(b, primaryKey(obj));
    final byte[] key = b.toByteArray();

    return IndexRow.CODEC.encodeToByteString(IndexRow.forKey(now, key))
        .toByteArray();
  }

  private static class IndexException extends RuntimeException {
    final OrmException cause;

    IndexException(OrmException err) {
      super(err);
      this.cause = err;
    }
  }
}
