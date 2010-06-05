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

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.nosql.NoSqlDatabase;

import java.util.concurrent.TimeUnit;

public abstract class GenericDatabase<T extends Schema, S extends GenericSchema, A extends GenericAccess>
    extends NoSqlDatabase<T, S, A> {
  private static final long DEFAULT_FOSSIL_AGE =
      TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * Initialize a new database and generate the implementation.
   *
   * @param schemaBaseType class that the generated Schema implementation should
   *        extend in order to provide data store connectivity.
   * @param accessBaseType class that the generated Access implementations
   *        should extend in order to provide single-relation access for each
   *        schema instance.
   * @param appSchema the application schema interface that must be implemented
   *        and constructed on demand.
   * @throws OrmException the schema cannot be created because of an annotation
   *         error in the interface definitions.
   */
  protected GenericDatabase(final Class<S> schemaBaseType,
      final Class<A> accessBaseType, final Class<T> appSchema)
      throws OrmException {
    super(schemaBaseType, accessBaseType, appSchema);
  }

  /**
   * Default number of milliseconds a transaction can appear to be open.
   * <p>
   * Secondary index rows that don't match their primary data object and that
   * are older than this age are removed from the system during a scan.
   *
   * @return milliseconds before considering a fossil index record is garbage
   *         and should be pruned. By default, 5 minutes.
   */
  public long getMaxFossilAge() {
    return DEFAULT_FOSSIL_AGE;
  }
}
