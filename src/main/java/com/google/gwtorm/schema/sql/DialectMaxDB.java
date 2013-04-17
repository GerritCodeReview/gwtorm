// Copyright (C) 2014 The Android Open Source Project
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

public class DialectMaxDB extends SqlDialect {

  public DialectMaxDB() {
    typeNames.put(Types.BIGINT, "FIXED (19,0)");
    typeNames.put(Types.LONGVARCHAR, "LONG UNICODE");
  }

  @Override
  public boolean canDetermineIndividualBatchUpdateCounts() {
    return false;
  }

  @Override
  public boolean canDetermineTotalBatchUpdateCount() {
    return true;
  }

  @Override
  public int executeBatch(PreparedStatement ps) throws SQLException {
    ps.executeBatch();
    return ps.getUpdateCount(); // total number of rows updated (on MaxDB)
  }

  @Override
  public Set<String> listSequences(Connection conn) throws SQLException {
    final Statement s = conn.createStatement();
    try {
      // lists sequences from schema associated with the current connection only
      final ResultSet rs =
          s.executeQuery("SELECT sequence_name FROM sequences");
      try {
        HashSet<String> sequences = new HashSet<String>();
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
  public OrmException convertError(String op, String entity, SQLException err) {
    int sqlstate = getSQLStateInt(err);
    if (sqlstate == 23000) { // UNIQUE CONSTRAINT VIOLATION
      int errorCode = err.getErrorCode();
      if (errorCode == 200 || errorCode == -20) { // Duplicate Key
        return new OrmDuplicateKeyException(entity, err);
      }
    }
    return super.convertError(op, entity, err);
  }

  @Override
  public String getNextSequenceValueSql(String seqname) {
    return "SELECT " + seqname + ".nextval FROM dual";
  }

  @Override
  public boolean handles(String url, Connection c) throws SQLException {
    return url.startsWith("jdbc:sapdb:");
  }

  @Override
  public void renameTable(StatementExecutor e, String from, String to)
      throws OrmException {
    final StringBuilder r = new StringBuilder();
    r.append("RENAME TABLE ");
    r.append(from);
    r.append(" TO ");
    r.append(to);
    e.execute(r.toString());
  }

}
