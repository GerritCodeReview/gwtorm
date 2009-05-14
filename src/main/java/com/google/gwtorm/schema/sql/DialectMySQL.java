// Copyright 2009 Google Inc.
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
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Sequence;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.SequenceModel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/** Dialect for <a href="http://www.mysql.com/">MySQL</a> */
public class DialectMySQL extends SqlDialect {
  public DialectMySQL() {
    types.put(String.class, new SqlStringTypeInfo() {
      @Override
      public String getSqlType(final ColumnModel col, final SqlDialect dialect) {
        final Column column = col.getColumnAnnotation();
        final StringBuilder r = new StringBuilder();
        final int type;

        if (column.length() <= 0) {
          r.append("VARCHAR(255)");
          if (col.isNotNull()) {
            r.append(" DEFAULT ''");
          }
        } else if (column.length() <= 255) {
          r.append("VARCHAR(" + column.length() + ")");
          if (col.isNotNull()) {
            r.append(" DEFAULT ''");
          }
        } else {
          r.append(dialect.getSqlTypeName(Types.LONGVARCHAR));
        }

        if (col.isNotNull()) {
          r.append(" NOT NULL");
        }

        return r.toString();
      }
    });
    types.put(java.sql.Timestamp.class, new SqlTimestampTypeInfo() {
      @Override
      public String getSqlDefault() {
        return "'1970-01-01 00:00:01'";
      }
    });
  }

  @Override
  public String getCreateSequenceSql(final SequenceModel seq) {
    final Sequence s = seq.getSequence();
    final StringBuilder r = new StringBuilder();
    r.append("CREATE TABLE ");
    r.append(seq.getSequenceName());
    r.append("(s SERIAL)");
    return r.toString();
  }

  @Override
  public String getNextSequenceValueSql(final String seqname) {
    return seqname;
  }

  @Override
  public long nextLong(final Connection conn, final String seqname)
      throws OrmException {
    try {
      final Statement st = conn.createStatement();
      try {
        st.execute("INSERT INTO " + seqname + "(s)VALUES(NULL)");
        final long r;
        final ResultSet rs = st.getGeneratedKeys();
        try {
          if (!rs.next()) {
            throw new SQLException("No result row for sequence query");
          }
          r = rs.getLong(1);
        } finally {
          rs.close();
        }
        st.execute("DELETE FROM " + seqname + " WHERE s=" + r);
        return r;
      } finally {
        st.close();
      }
    } catch (SQLException e) {
      throw convertError("sequence", seqname, e);
    }
  }
}
