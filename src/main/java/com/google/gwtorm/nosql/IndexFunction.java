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

/**
 * A function to produce a NoSQL secondary index key from an object.
 *
 * <p>An index function computes a row key for a secondary index table by appending the relevant
 * values to the builder's internal buffer in the order they are referenced in the query.
 *
 * <p>Typically an IndexFunction is automatically code generated at runtime by {@link
 * IndexFunctionGen}.
 *
 * @param <T> type of the object the index record references.
 */
public abstract class IndexFunction<T> {
  /** @return name of this index, should be unique within the relation. */
  public abstract String getName();

  /**
   * Should this object exist in the index?
   *
   * <p>Objects that shouldn't appear in this index are skipped because field values are currently
   * {@code null}, or because one or more field values do not match the constants used in the query
   * that defines the index.
   *
   * @param object the object to read fields from.
   * @return true if the object should be indexed by this index.
   */
  public abstract boolean includes(T object);

  /**
   * Encodes the current values from the object into the index buffer.
   *
   * @param dst the buffer to append the indexed field value(s) onto.
   * @param object the object to read current field values from.
   */
  public abstract void encode(IndexKeyBuilder dst, T object);
}
