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

import com.google.gwtorm.client.Column;
import com.google.gwtorm.protobuf.CodecFactory;
import com.google.gwtorm.protobuf.ProtobufCodec;

/**
 * Data value stored in a NoSQL secondary index row.
 * <p>
 * Instances of this object can be used inside of the data portion of a
 * secondary index row, and may either contain the key of the primary data row,
 * or a copy of the primary object data.
 * <p>
 * The {@link #timestamp} field can be used to fossil collect secondary index
 * rows that no longer match the primary data row and which are older than the
 * longest expected transaction. These fossil rows may have occurred due to an
 * aborted, but partially applied transaction.
 */
public class IndexRow {
  /** Standard encoder/decoder for this class. */
  public static final ProtobufCodec<IndexRow> CODEC =
      CodecFactory.encoder(IndexRow.class);

  /**
   * Create an index row to reference the primary data row by key.
   *
   * @param update time of the update.
   * @param key the key to reference.
   * @return the new index row.
   */
  public static IndexRow forKey(long update, byte[] key) {
    IndexRow r = new IndexRow();
    r.timestamp = update;
    r.dataKey = key;
    return r;
  }

  /**
   * Clock of the last time this index row was touched.
   * <p>
   * Invalid rows older than a certain time interval may be subject to automatic
   * background pruning during data retrieval operations.
   */
  @Column(id = 1)
  protected long timestamp;

  /** Key within the same relation that holds the actual data. */
  @Column(id = 2, notNull = false)
  protected byte[] dataKey;

  /** Stale copy of the data. */
  @Column(id = 3, notNull = false)
  protected byte[] dataCopy;

  /** @return get the timestamp of the row. */
  public long getTimestamp() {
    return timestamp;
  }

  /** @return get the primary key data; or {@code null}. */
  public byte[] getDataKey() {
    return dataKey;
  }

  /** @return get the copy of the primary data; or {@code null}. */
  public byte[] getDataCopy() {
    return dataCopy;
  }
}
