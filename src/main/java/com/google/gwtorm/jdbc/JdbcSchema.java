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

package com.google.gwtorm.jdbc;

import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.OrmRunnable;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.client.Transaction;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.SequenceModel;
import com.google.gwtorm.schema.sql.SqlDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Internal base class for implementations of {@link Schema}. */
public abstract class JdbcSchema implements Schema {
  private static final int MAX_TRIES = 10;
  private final Database<?> dbDef;
  private Connection conn;

  protected JdbcSchema(final Database<?> d, final Connection c) {
    dbDef = d;
    conn = c;
  }

  public final Connection getConnection() {
    return conn;
  }

  public final SqlDialect getDialect() {
    return dbDef.getDialect();
  }

  public <T, S extends Schema> T run(final OrmRunnable<T, S> task)
      throws OrmException {
    for (int attempts = 1;; attempts++) {
      try {
        final Transaction txn = beginTransaction();
        try {
          return task.run((S) this, txn, attempts > 1);
        } finally {
          txn.commit();
        }
      } catch (OrmConcurrencyException err) {
        // If the commit failed, our implementation rolled back automatically.
        //
        if (attempts < MAX_TRIES) {
          continue;
        }
        throw err;
      }
    }
  }

  public void createSchema() throws OrmException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    try {
      final Statement stmt;

      stmt = getConnection().createStatement();
      try {
        for (final SequenceModel s : model.getSequences()) {
          stmt.execute(s.getCreateSequenceSql(dialect));
        }
        for (final RelationModel r : model.getRelations()) {
          stmt.execute(r.getCreateTableSql(dialect));
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Schema creation failure", e);
    }
  }

  protected long nextLong(final String query) throws OrmException {
    return getDialect().nextLong(getConnection(), query);
  }

  public Transaction beginTransaction() {
    return new JdbcTransaction(this);
  }

  public void close() {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException err) {
        // TODO Handle an exception while closing a connection
      }
      conn = null;
    }
  }
}
