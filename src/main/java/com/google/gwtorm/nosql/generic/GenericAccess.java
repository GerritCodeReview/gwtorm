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

import com.google.gwtorm.client.Key;
import com.google.gwtorm.nosql.IndexFunction;
import com.google.gwtorm.nosql.IndexKeyBuilder;
import com.google.gwtorm.nosql.IndexRow;
import com.google.gwtorm.nosql.NoSqlAccess;
import com.google.gwtorm.server.AbstractResultSet;
import com.google.gwtorm.server.Access;
import com.google.gwtorm.server.AtomicUpdate;
import com.google.gwtorm.server.ListResultSet;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmDuplicateKeyException;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.ResultSet;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
   *
   * @param key the primary key instance; must not be null.
   * @return the entity; null if no entity has this key.
   * @throws OrmException the data lookup failed.
   * @throws OrmDuplicateKeyException more than one row was identified in the
   *         key scan.
   */
  @Override
  public T get(K key) throws OrmException, OrmDuplicateKeyException {
    byte[] bin = db.fetchRow(dataRowKey(key));
    if (bin != null) {
      T obj = getObjectCodec().decode(bin);
      cache().put(primaryKey(obj), bin);
      return obj;
    } else {
      return null;
    }
  }

  @Override
  public ResultSet<T> get(final Iterable<K> keys) throws OrmException {
    final ResultSet<Row> rs = db.fetchRows(new Iterable<byte[]>() {
      @Override
      public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {
          private final Iterator<K> i = keys.iterator();

          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public byte[] next() {
            return dataRowKey(i.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    });

    final Iterator<Row> i = rs.iterator();
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
   * Scan a range of keys from the data rows and return any matching objects.
   *
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return.
   * @param order if true the order will be preserved, false if the result order
   *        order can be arbitrary.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  @Override
  protected ResultSet<T> scanPrimaryKey(byte[] fromKey, byte[] toKey,
      int limit, boolean order) throws OrmException {
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

    final ResultSet<Row> rs = db.scan(fromKey, toKey, limit, order);
    final Iterator<Row> i = rs.iterator();

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
   * @param order if true the order will be preserved, false if the result order
   *        order can be arbitrary.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  @Override
  protected ResultSet<T> scanIndex(IndexFunction<T> idx, byte[] fromKey,
      byte[] toKey, int limit, boolean order) throws OrmException {
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
      List<CandidateRow> scanned;
      if (0 < limit) {
        scanned = new ArrayList<CandidateRow>(limit);
      } else {
        scanned = new ArrayList<CandidateRow>();
      }

      boolean needData = false;
      for (Row ent : db.scan(lastKey, toKey, limit, order)) {
        byte[] idxKey = ent.getKey();
        IndexRow idxRow = IndexRow.CODEC.decode(ent.getValue());
        CandidateRow row = new CandidateRow(idxKey, idxRow);
        scanned.add(row);
        needData |= !row.hasData();
        lastKey = idxKey;
      }

      if (needData) {
        // At least one row from the index didn't have a cached copy of the
        // object stored within. For these rows we need to fetch the real
        // data row and join it against the index information.
        //
        HashMap<ByteString, CandidateRow> byKey =
            new HashMap<ByteString, CandidateRow>();
        List<byte[]> toFetch = new ArrayList<byte[]>(scanned.size());

        for (CandidateRow idxRow : scanned) {
          if (!idxRow.hasData()) {
            IndexKeyBuilder pk = new IndexKeyBuilder();
            pk.add(getRelationName());
            pk.delimiter();
            pk.addRaw(idxRow.getDataKey());
            byte[] key = pk.toByteArray();

            byKey.put(ByteString.copyFrom(key), idxRow);
            toFetch.add(key);
          }
        }

        for (Row objRow : db.fetchRows(toFetch)) {
          CandidateRow idxRow = byKey.get(ByteString.copyFrom(objRow.getKey()));
          if (idxRow != null) {
            idxRow.setData(objRow.getValue());
          }
        }

        for (CandidateRow idxRow : scanned) {
          // If we have no data present and this row is stale enough,
          // drop the row out of the index.
          //
          if (!idxRow.hasData()) {
            db.maybeFossilCollectIndexRow(now, idxRow.getIndexKey(), //
                idxRow.getIndexRow());
            continue;
          }

          // Verify the object still matches the predicate of the index.
          // If it does, include it in the result. Otherwise, maybe we
          // should drop it from the index.
          //
          byte[] bin = idxRow.getData();
          final T obj = getObjectCodec().decode(bin);
          if (matches(idx, obj, idxRow.getIndexKey())) {
            cache().put(primaryKey(obj), bin);
            res.add(obj);
            if (limit > 0 && res.size() == limit) {
              break SCAN;
            }
          } else {
            db.maybeFossilCollectIndexRow(now, idxRow.getIndexKey(), //
                idxRow.getIndexRow());
          }
        }
      } else {
        // All of the rows are using a cached copy of the object. We can
        // simply decode and produce those without further validation.
        //
        for (CandidateRow idxRow : scanned) {
          byte[] bin = idxRow.getData();
          T obj = getObjectCodec().decode(bin);
          cache().put(primaryKey(obj), bin);
          res.add(obj);
          if (limit > 0 && res.size() == limit) {
            break SCAN;
          }
        }
      }

      // If we have no limit we scanned everything, so break out.
      // If scanned < limit, we saw every index row that might be
      // a match, and no further rows would exist.
      //
      if (limit == 0 || scanned.size() < limit) {
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

  @Override
  public void insert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      insertOne(obj);
    }
    db.flush();
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
    db.flush();
  }

  @Override
  public void upsert(Iterable<T> instances) throws OrmException {
    for (T obj : instances) {
      upsertOne(obj, false);
    }
    db.flush();
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
   * (such as {@link #scanIndex(IndexFunction, byte[], byte[], int, boolean)}
   * above) will ignore these rows for a short time period.
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
    db.flush();
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

    return IndexRow.CODEC.encodeToByteArray(IndexRow.forKey(now, key));
  }

  private static class IndexException extends RuntimeException {
    final OrmException cause;

    IndexException(OrmException err) {
      super(err);
      this.cause = err;
    }
  }
}
