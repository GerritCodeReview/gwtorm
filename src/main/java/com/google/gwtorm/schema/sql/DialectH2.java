// Copyright 2008 Google Inc.
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

package com.google.gwtorm.schema.sql;

import com.google.gwtorm.client.OrmDuplicateKeyException;
import com.google.gwtorm.client.OrmException;

import java.sql.SQLException;

/** Dialect for <a href="http://www.h2database.com/">H2</a> */
public class DialectH2 extends SqlDialect {
  @Override
  public OrmException convertError(final String op, final String entity,
      final SQLException err) {
    switch (getSQLStateInt(err)) {
      case 23001: // UNIQUE CONSTRAINT VIOLATION
        return new OrmDuplicateKeyException(entity, err);

      case 23000: // CHECK CONSTRAINT VIOLATION
      default:
        return super.convertError(op, entity, err);
    }
  }

  @Override
  public String getNextSequenceValueSql(final String seqname) {
    return "SELECT NEXT VALUE FOR " + seqname;
  }
}
