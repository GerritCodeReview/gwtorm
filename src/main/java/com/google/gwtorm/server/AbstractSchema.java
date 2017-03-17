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

package com.google.gwtorm.server;

/** Base implementation any generated schema must implement. */
public abstract class AbstractSchema implements Schema {
  @Override
  public void commit() throws OrmException {
    // Do nothign by default.
  }

  @Override
  public void rollback() throws OrmException {
    // Do nothign by default.
  }

  /**
   * Obtain the next unique value from a pool of available numbers.
   *
   * <p>Frequently the next number will be just an increment of a global counter, but may be spread
   * across multiple counter ranges to increase concurrency.
   *
   * @param poolName unique name of the counter within the schema. The underlying storage system
   *     should use this to identify the counter pool to obtain the next value from.
   * @return a new unique value.
   * @throws OrmException a value cannot be reserved for the caller, or the pool has been exhausted
   *     and no new values are available.
   */
  protected abstract long nextLong(String poolName) throws OrmException;
}
