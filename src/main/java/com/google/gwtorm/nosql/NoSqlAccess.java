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

package com.google.gwtorm.nosql;

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.client.impl.AbstractAccess;
import com.google.gwtorm.protobuf.ProtobufCodec;

/** Internal base class for implementations of {@link Access}. */
public abstract class NoSqlAccess<T, K extends Key<?>> extends
    AbstractAccess<T, K> {
  protected NoSqlAccess(final NoSqlSchema s) {
  }

  /**
   * Scan a range of keys and return any matching objects.
   * <p>
   * All NoSQL implementations must provide their own variant of this method.
   * <p>
   * To fetch a single record with a scan, set {@code toKey} to the same array
   * as {@code fromKey}, but append a trailing NUL byte (0x00). The caller
   * should validate that the returned ResultSet contains no more than 1 row.
   *
   * @param indexName name of the index the scan occurs over.
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  protected abstract ResultSet<T> scan(String indexName, byte[] fromKey,
      byte[] toKey, int limit) throws OrmException;

  protected IndexFunction<T> getQueryIndex(String indexName)
      throws OrmException {
    for (IndexFunction<T> f : getQueryIndexes()) {
      if (indexName.equals(f.getName())) {
        return f;
      }
    }
    if (indexName.equals(getKeyIndex().getName())) {
      return getKeyIndex();
    }
    throw new OrmException("No index named " + indexName);
  }

  // -- These are all provided by AccessGen when builds a subclass --

  protected abstract String getRelationName();

  protected abstract ProtobufCodec<T> getObjectCodec();

  protected abstract IndexFunction<T> getKeyIndex();

  protected abstract IndexFunction<T>[] getQueryIndexes();

  protected abstract void encodeKey(IndexKeyBuilder dst, K key);
}
