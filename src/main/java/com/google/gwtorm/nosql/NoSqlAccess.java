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
   * Scan a range of keys from the data rows and return any matching objects.
   * <p>
   * All NoSQL implementations must provide their own variant of this method.
   * <p>
   * To fetch a single record with a scan, set {@code toKey} to the same array
   * as {@code fromKey}, but append a trailing NUL byte (0x00). The caller
   * should validate that the returned ResultSet contains no more than 1 row.
   *
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return, 0 for unlimited.
   * @param order if true the order will be preserved, false if the result order
   *        order can be arbitrary.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  protected abstract ResultSet<T> scanPrimaryKey(byte[] fromKey, byte[] toKey,
      int limit, boolean order) throws OrmException;

  /**
   * Scan a range of keys and return any matching objects.
   * <p>
   * All NoSQL implementations must provide their own variant of this method.
   * <p>
   * To fetch a single record with a scan, set {@code toKey} to the same array
   * as {@code fromKey}, but append a trailing NUL byte (0x00). The caller
   * should validate that the returned ResultSet contains no more than 1 row.
   *
   * @param index definition of the index the scan occurs over.
   * @param fromKey key to start the scan on. This is inclusive.
   * @param toKey key to stop the scan on. This is exclusive.
   * @param limit maximum number of results to return, 0 for unlimited.
   * @param order if true the order will be preserved, false if the result order
   *        order can be arbitrary.
   * @return result set for the requested range. The result set may be lazily
   *         filled, or filled completely.
   * @throws OrmException an error occurred preventing the scan from completing.
   */
  protected abstract ResultSet<T> scanIndex(IndexFunction<T> index,
      byte[] fromKey, byte[] toKey, int limit, boolean order)
      throws OrmException;

  // -- These are all provided by AccessGen when it builds a subclass --

  /** @return encoder/decoder for the object data. */
  protected abstract ProtobufCodec<T> getObjectCodec();

  /**
   * Get the indexes that support query functions.
   * <p>
   * This array may be a subset of the total query functions. This can occur
   * when two or more queries can be efficiently answered by performing a range
   * scan over the same index.
   *
   * @return indexes needed to support queries.
   */
  protected abstract IndexFunction<T>[] getIndexes();

  /**
   * Encode the primary key of the object.
   *
   * @param dst builder the key components will be added into.
   * @param key the object primary key.
   */
  protected abstract void encodePrimaryKey(IndexKeyBuilder dst, K key);
}
