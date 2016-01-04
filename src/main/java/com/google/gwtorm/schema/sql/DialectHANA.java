// Copyright (C) 2015 The Android Open Source Project
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

import com.google.gwtorm.client.Column;
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
import java.util.Locale;
import java.util.Set;

public class DialectHANA extends SqlDialect {

  public DialectHANA() {
    types.put(String.class, new SqlStringTypeInfo() {
      @Override
      public String getSqlType(final ColumnModel col, final SqlDialect dialect) {
        final Column column = col.getColumnAnnotation();
        final StringBuilder r = new StringBuilder();

        if (column.length() <= 0) {
          r.append("NVARCHAR(255)");
          if (col.isNotNull()) {
            r.append(" DEFAULT ''");
          }
        } else if (column.length() <= 5000) {
          r.append("NVARCHAR(" + column.length() + ")");
          if (col.isNotNull()) {
            r.append(" DEFAULT ''");
          }
        } else {
          r.append("NCLOB");
        }

        if (col.isNotNull()) {
          r.append(" NOT NULL");
        }

        return r.toString();
      }
    });
    types.put(Boolean.TYPE, new SqlBooleanTypeInfo() {
      @Override
      public String getCheckConstraint(final ColumnModel column,
          final SqlDialect dialect) {
        return null;
      }


    });
    typeNames.put(Types.INTEGER, "INTEGER");
  }

  @Override
  public void addColumn(StatementExecutor e, String tableName, ColumnModel col)
      throws OrmException {
    final StringBuilder r = new StringBuilder();
    r.append("ALTER TABLE ");
    r.append(tableName);
    r.append(" ADD (");
    r.append(col.getColumnName());
    r.append(" ");
    r.append(getSqlTypeInfo(col).getSqlType(col, this));
    r.append(")");
    e.execute(r.toString());
  }

  @Override
  public void dropColumn(StatementExecutor e, String tableName, String column)
      throws OrmException {
    final StringBuilder r = new StringBuilder();
    r.append("ALTER TABLE ");
    r.append(tableName);
    r.append(" DROP (");
    r.append(column);
    r.append(")");
    e.execute(r.toString());
  }

  @Override
  public boolean handles(String url, Connection c) throws SQLException {
    return url.startsWith("jdbc:sap://");
  }

  @Override
  public Set<String> listSequences(Connection db) throws SQLException {
    return listNamesFromSystemTable(db, "SEQUENCE_NAME", "SEQUENCES");
  }

  @Override
  public Set<String> listTables(Connection db) throws SQLException {
    return listNamesFromSystemTable(db, "TABLE_NAME", "TABLES");
  }

  @Override
  public Set<String> listIndexes(Connection db, String tableName)
      throws SQLException {
    return listNamesFromSystemTable(db, "INDEX_NAME", "INDEXES");
  }

  @Override
  public Set<String> listColumns(Connection db, String tableName)
      throws SQLException {
    final PreparedStatement s =
        db.prepareStatement("SELECT COLUMN_NAME FROM TABLE_COLUMNS"
            + " WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = ?");
    try {
      s.setString(1, tableName.toUpperCase(Locale.US));
      final ResultSet rs = s.executeQuery();
      try {
        return names(rs);
      } finally {
        rs.close();
      }
    } finally {
      s.close();
    }
  }

  @Override
  public boolean isStatementDelimiterSupported() {
    return false;
  }

  private static Set<String> names(final ResultSet rs) throws SQLException {
    HashSet<String> names = new HashSet<String>();
    while (rs.next()) {
      names.add(rs.getString(1).toLowerCase());
    }
    return names;
  }

  private static Set<String> listNamesFromSystemTable(Connection db,
      String columnName, String tableName) throws SQLException {
    final Statement s = db.createStatement();
    try {
      final ResultSet rs =
          s.executeQuery("SELECT " + columnName + " FROM " + tableName
              + " WHERE SCHEMA_NAME = CURRENT_SCHEMA");
      try {
        return names(rs);
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
    final StringBuilder r = new StringBuilder();
    r.append("RENAME TABLE ");
    r.append(from);
    r.append(" TO ");
    r.append(to);
    r.append(" ");
    e.execute(r.toString());
  }

  @Override
  public void appendTableType(StringBuilder sqlBuffer) {
    sqlBuffer.append("COLUMN TABLE");
  }

  @Override
  public OrmException convertError(String op, String entity, SQLException err) {
    int sqlstate = getSQLStateInt(err);
    if (sqlstate == 23000) { // UNIQUE CONSTRAINT VIOLATION
      int errorCode = err.getErrorCode();
      if (errorCode == 144 || errorCode == 301) { // Duplicate Key
        return new OrmDuplicateKeyException(entity, err);
      }
    }
    return super.convertError(op, entity, err);
  }

  @Override
  public void renameColumn(StatementExecutor e, String tableName,
      String fromColumn, ColumnModel col) throws OrmException {
    final StringBuilder s = new StringBuilder();
    s.append("RENAME COLUMN ");
    s.append(tableName).append(".").append(fromColumn);
    s.append(" TO ");
    s.append(col.getColumnName());
    e.execute(s.toString());
  }

  @Override
  protected String getNextSequenceValueSql(String seqname) {
    return "SELECT " + seqname + ".nextval FROM dummy";
  }
}
