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
 * A single slice of an incrementing counter.
 * <p>
 * <b>This shard class is not thread safe.</b> Implementors using this type must
 * perform synchronization through external mechanisms such as a row-level lock.
 * <p>
 * NoSQL implementations can use this object to store counters and keep track of
 * their values within {@code nextLong(String)}. To improve allocation
 * performance counters may be sliced into shards, with allocation coming out of
 * a randomly selected shard, and each shard being replenished from a master
 * shard when it {@link #isEmpty()}.
 */
public class CounterShard {
  /** Standard encoder/decoder for this class. */
  public static final ProtobufCodec<CounterShard> CODEC =
      CodecFactory.encoder(CounterShard.class);

  /** Current value in this shard, this is the next to assign out. */
  @Column(id = 1)
  protected long current;

  /** Maximum value, the shard cannot hand out this value. */
  @Column(id = 2)
  protected long max;

  protected CounterShard() {
  }

  /**
   * Create a new shard with a specific starting value, with no maximum.
   *
   * @param next the first value this shard will hand out.
   */
  public CounterShard(long next) {
    this(next, Long.MAX_VALUE);
  }

  /**
   * Create a new shard with a specific starting point and maximum.
   *
   * @param next the first value this shard will hand out.
   * @param max the highest value the shard will stop at. The shard will not
   *        actually hand out this value.
   */
  public CounterShard(long next, long max) {
    this.current = next;
    this.max = max;
  }

  /** @return true if this shard cannot hand out any more values. */
  public boolean isEmpty() {
    return current == max;
  }

  /**
   * Obtain the next value from this shard.
   *
   * @return the next value
   * @throws IllegalStateException the shard {@link #isEmpty()} and cannot hand
   *         out any more values.
   */
  public long next() {
    if (isEmpty()) {
      throw new IllegalStateException("Counter shard out of values");
    }
    return current++;
  }
}
