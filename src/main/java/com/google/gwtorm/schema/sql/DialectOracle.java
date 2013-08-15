// Copyright 2013 The Android Open Source Project
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

public class DialectOracle extends SqlDialect {

  public DialectOracle() {
    typeNames.put(Types.BIGINT, "NUMBER(19,0)");
    typeNames.put(Types.LONGVARCHAR, "CLOB");
  }

  @Override
  public boolean handles(String url, Connection c) throws SQLException {
    return url.startsWith("jdbc:oracle:");
  }

  @Override
  public boolean canDetermineIndividualBatchUpdateCounts() {
    return false;
  }
  @Override
  public boolean canDetermineTotalBatchUpdateCount() {
    return false;
  }

  @Override
  public Set<String> listTables(final Connection db) throws SQLException {
    Statement s = db.createStatement();
    try {
      ResultSet rs = s.executeQuery("SELECT table_name FROM user_tables");
      try {
        Set<String> tables = new HashSet<String>();
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
    PreparedStatement s = db.prepareStatement("SELECT distinct index_name "
        + "FROM user_indexes WHERE table_name = ?");
    try {
      s.setString(1, tableName.toUpperCase());
      ResultSet rs = s.executeQuery();
      try {
        Set<String> indexes = new HashSet<String>();
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
  public Set<String> listSequences(Connection db) throws SQLException {
    Statement s = db.createStatement();
    try {
      ResultSet rs = s.executeQuery("SELECT sequence_name FROM user_sequences");
      try {
        Set<String> sequences = new HashSet<String>();
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
  public void addColumn(StatementExecutor stmt, String tableName,
      ColumnModel col) throws OrmException {
    final StringBuilder r = new StringBuilder();
    r.append("ALTER TABLE ");
    r.append(tableName);
    r.append(" ADD ");
    r.append(col.getColumnName());
    r.append(" ");
    r.append(getSqlTypeInfo(col).getSqlType(col, this));
    stmt.execute(r.toString());
  }

  @Override
  public void renameColumn(StatementExecutor e, String tableName,
      String fromColumn, ColumnModel col) throws OrmException {
    StringBuffer sb = new StringBuffer();
    sb.append("ALTER TABLE ");
    sb.append(tableName);
    sb.append(" RENAME COLUMN ");
    sb.append(fromColumn);
    sb.append(" TO ");
    sb.append(col.getColumnName());
    e.execute(sb.toString());
  }

  @Override
  public String getNextSequenceValueSql(String seqname) {
    return "SELECT " + seqname + ".nextval FROM dual";
  }

  @Override
  public boolean selectHasLimit() {
    return false;
  }

  @Override
  public boolean isStatementDelimiterSupported() {
    return false;
  }
}
