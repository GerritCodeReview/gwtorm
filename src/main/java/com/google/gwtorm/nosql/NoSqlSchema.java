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

import com.google.gwtorm.server.AbstractSchema;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;
import com.google.gwtorm.server.StatementExecutor;

/** Internal base class for implementations of {@link Schema}. */
public abstract class NoSqlSchema extends AbstractSchema {
  protected NoSqlSchema(final NoSqlDatabase<?, ?, ?> d) {
  }

  @Override
  public void pruneSchema(StatementExecutor e) throws OrmException {
    // Assume no action is required in a default NoSQL environment.
  }

  @Override
  public void updateSchema(StatementExecutor e) throws OrmException {
    // Assume no action is required in a default NoSQL environment.
  }
}
