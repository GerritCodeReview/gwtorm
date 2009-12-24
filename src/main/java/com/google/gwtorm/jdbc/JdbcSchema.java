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
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.SequenceModel;
import com.google.gwtorm.schema.sql.SqlDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

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

  public void updateSchema() throws OrmException {
    try {
      createSequences();
      createRelations();

      for (final RelationModel rel : dbDef.getSchemaModel().getRelations()) {
        addColumns(rel);
      }
    } catch (SQLException e) {
      throw new OrmException("Schema update failure", e);
    }
  }

  private void createSequences() throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      Set<String> have = dialect.listSequences(getConnection());
      for (final SequenceModel s : model.getSequences()) {
        if (!have.contains(s.getSequenceName().toLowerCase())) {
          stmt.execute(s.getCreateSequenceSql(dialect));
        }
      }
    } finally {
      stmt.close();
    }
  }

  private void createRelations() throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      Set<String> have = dialect.listTables(getConnection());
      for (final RelationModel r : model.getRelations()) {
        if (!have.contains(r.getRelationName().toLowerCase())) {
          stmt.execute(r.getCreateTableSql(dialect));
        }
      }
    } finally {
      stmt.close();
    }
  }

  private void addColumns(final RelationModel rel) throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      Set<String> have = dialect.listColumns( //
          getConnection(), rel.getRelationName().toLowerCase());
      for (final ColumnModel c : rel.getColumns()) {
        if (!have.contains(c.getColumnName().toLowerCase())) {
          dialect.addColumn(stmt, rel.getRelationName(), c);
        }
      }
    } finally {
      stmt.close();
    }
  }

  public void renameField(String table, String from, String to)
      throws OrmException {
    final RelationModel rel = findRelationModel(table);
    if (rel == null) {
      throw new OrmException("Relation " + table + " not defined");
    }
    final ColumnModel col = rel.getField(to);
    if (col == null) {
      throw new OrmException("Relation " + table + " does not have " + to);
    }
    try {
      final Statement s = getConnection().createStatement();
      try {
        getDialect().renameColumn(s, table, from, col);
      } finally {
        s.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Cannot rename " + table + "." + from + " to "
          + to, e);
    }
  }

  private RelationModel findRelationModel(String table) throws OrmException {
    for (final RelationModel rel : dbDef.getSchemaModel().getRelations()) {
      if (table.equalsIgnoreCase(rel.getRelationName())) {
        return rel;
      }
    }
    return null;
  }

  public void pruneSchema() throws OrmException {
    try {
      pruneSequences();
      pruneRelations();

      for (final RelationModel rel : dbDef.getSchemaModel().getRelations()) {
        pruneColumns(rel);
      }
    } catch (SQLException e) {
      throw new OrmException("Schema prune failure", e);
    }
  }

  private void pruneSequences() throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      HashSet<String> want = new HashSet<String>();
      for (final SequenceModel s : model.getSequences()) {
        want.add(s.getSequenceName().toLowerCase());
      }
      for (final String sequence : dialect.listSequences(getConnection())) {
        if (!want.contains(sequence)) {
          stmt.execute(dialect.getDropSequenceSql(sequence));
        }
      }
    } finally {
      stmt.close();
    }
  }

  private void pruneRelations() throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      HashSet<String> want = new HashSet<String>();
      for (final RelationModel r : model.getRelations()) {
        want.add(r.getRelationName().toLowerCase());
      }
      for (final String table : dialect.listTables(getConnection())) {
        if (!want.contains(table)) {
          stmt.execute("DROP TABLE " + table);
        }
      }
    } finally {
      stmt.close();
    }
  }

  private void pruneColumns(final RelationModel rel) throws SQLException {
    final SqlDialect dialect = dbDef.getDialect();
    final SchemaModel model = dbDef.getSchemaModel();
    final Statement stmt = getConnection().createStatement();
    try {
      HashSet<String> want = new HashSet<String>();
      for (final ColumnModel c : rel.getColumns()) {
        want.add(c.getColumnName().toLowerCase());
      }
      for (String column : dialect.listColumns( //
          getConnection(), rel.getRelationName().toLowerCase())) {
        if (!want.contains(column)) {
          dialect.dropColumn(stmt, rel.getRelationName(), column);
        }
      }
    } finally {
      stmt.close();
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
