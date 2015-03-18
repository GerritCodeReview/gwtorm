// Copyright 2015 The Android Open Source Project
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

import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.server.OrmDuplicateKeyException;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.StatementExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/** Dialect for DB2 */
public class DialectDB2 extends SqlDialect {

  public DialectDB2() {
    typeNames.put(Types.LONGVARCHAR, "CLOB");
  }

  @Override
  public boolean handles(String url, Connection c) {
    return url.startsWith("jdbc:db2:");
  }

  @Override
  public OrmException convertError(String op, String entity,
      SQLException err) {
    switch (getSQLStateInt(err)) {
      case 23505: // DUPLICATE_KEY_1
        return new OrmDuplicateKeyException(entity, err);

      default:
        return super.convertError(op, entity, err);
    }
  }

  @Override
  protected String getNextSequenceValueSql(String seqname) {
    return "VALUES NEXT VALUE FOR " + seqname;
  }

  @Override
  public Set<String> listSequences(Connection db) throws SQLException {
    Statement s = db.createStatement();
    try {
      ResultSet rs =
          s.executeQuery("SELECT SEQNAME"
              + " FROM SYSCAT.SEQUENCES"
              + " WHERE SEQSCHEMA = CURRENT_SCHEMA");
      try {
        HashSet<String> sequences = new HashSet<>();
        while (rs.next()) {
          sequences.add(rs.getString(1).toLowerCase());
        }
        return sequences;
      } finally {
        rs.close();
      }
    } finally {
      s.close();
    }
  }

  @Override
  public Set<String> listTables(Connection db) throws SQLException {
    Statement s = db.createStatement();
    try {
      ResultSet rs = s.executeQuery("SELECT TABNAME"
          + " FROM SYSCAT.TABLES"
          + " WHERE TABSCHEMA = CURRENT_SCHEMA");
      try {
        Set<String> tables = new HashSet<>();
        while (rs.next()) {
          tables.add(rs.getString(1).toLowerCase());
        }
        return tables;
      } finally {
        rs.close();
      }
    } finally {
      s.close();
    }
  }

  @Override
  public Set<String> listIndexes(final Connection db, String tableName)
      throws SQLException {
    PreparedStatement s = db.prepareStatement("SELECT distinct INDNAME"
        + " FROM syscat.indexes WHERE TABNAME = ? AND TABSCHEMA = CURRENT_SCHEMA");
    try {
      s.setString(1, tableName.toUpperCase());
      ResultSet rs = s.executeQuery();
      try {
        Set<String> indexes = new HashSet<>();
        while (rs.next()) {
          indexes.add(rs.getString(1).toLowerCase());
        }
        return indexes;
      } finally {
        rs.close();
      }
    } finally {
      s.close();
    }
  }

  @Override
  public void renameTable(StatementExecutor e, String from, String to)
      throws OrmException {
    StringBuilder r = new StringBuilder();
    r.append("RENAME TABLE ");
    r.append(from);
    r.append(" TO ");
    r.append(to);
    e.execute(r.toString());
  }

  @Override
  public void renameColumn(StatementExecutor stmt, String tableName,
      String fromColumn, ColumnModel col) throws OrmException {
    StringBuilder r = new StringBuilder();
    r.append("ALTER TABLE ");
    r.append(tableName);
    r.append(" RENAME COLUMN ");
    r.append(fromColumn);
    r.append(" TO ");
    r.append(col.getColumnName());
    stmt.execute(r.toString());
  }
}
