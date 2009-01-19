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

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Sequence;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.SequenceModel;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public abstract class SqlDialect {
  protected final Map<Class<?>, SqlTypeInfo> types;
  protected final Map<Integer, String> typeNames;

  protected SqlDialect() {
    types = new HashMap<Class<?>, SqlTypeInfo>();
    types.put(Boolean.TYPE, new SqlBooleanTypeInfo());
    types.put(Short.TYPE, new SqlShortTypeInfo());
    types.put(Integer.TYPE, new SqlIntTypeInfo());
    types.put(Long.TYPE, new SqlLongTypeInfo());
    types.put(Character.TYPE, new SqlCharTypeInfo());
    types.put(String.class, new SqlStringTypeInfo());
    types.put(java.sql.Date.class, new SqlDateTypeInfo());
    types.put(java.sql.Timestamp.class, new SqlTimestampTypeInfo());
    types.put(byte[].class, new SqlByteArrayTypeInfo());

    typeNames = new HashMap<Integer, String>();
    typeNames.put(Types.VARBINARY, "BLOB");
    typeNames.put(Types.DATE, "DATE");
    typeNames.put(Types.SMALLINT, "SMALLINT");
    typeNames.put(Types.INTEGER, "INT");
    typeNames.put(Types.BIGINT, "BIGINT");
    typeNames.put(Types.LONGVARCHAR, "TEXT");
    typeNames.put(Types.TIMESTAMP, "TIMESTAMP");
  }

  /** Select a better dialect definition for this connection */
  public SqlDialect refine(final Connection c) throws SQLException {
    return this;
  }

  public String getSqlTypeName(final int typeCode) {
    final String r = typeNames.get(typeCode);
    return r != null ? r : "UNKNOWNTYPE";
  }

  public SqlTypeInfo getSqlTypeInfo(final ColumnModel col) {
    return getSqlTypeInfo(col.getPrimitiveType());
  }

  public SqlTypeInfo getSqlTypeInfo(final Class<?> t) {
    return types.get(t);
  }

  public String getParameterPlaceHolder(final int nthParameter) {
    return "?";
  }

  public boolean selectHasLimit() {
    return true;
  }

  protected static String getSQLState(SQLException err) {
    String ec;
    SQLException next = err;
    do {
      ec = next.getSQLState();
      next = next.getNextException();
    } while (ec == null && next != null);
    return ec;
  }

  protected static int getSQLStateInt(SQLException err) {
    final String s = getSQLState(err);
    if (s != null) {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        return -1;
      }
    }
    return 0;
  }

  /**
   * Convert a driver specific exception into an {@link OrmException}.
   * 
   * @param op short description of the operation, e.g. "update" or "fetch".
   * @param entity name of the entity being accessed by the operation.
   * @param err the driver specific exception.
   * @return an OrmException the caller can throw.
   */
  public OrmException convertError(final String op, final String entity,
      final SQLException err) {
    return new OrmException(op + " failure on " + entity, err);
  }

  public String getCreateSequenceSql(final SequenceModel seq) {
    final Sequence s = seq.getSequence();
    final StringBuilder r = new StringBuilder();
    r.append("CREATE SEQUENCE ");
    r.append(seq.getSequenceName());

    if (s.startWith() > 0) {
      r.append(" START WITH ");
      r.append(s.startWith());
    }

    if (s.cache() > 0) {
      r.append(" CACHE ");
      r.append(s.cache());
    }

    return r.toString();
  }

  public abstract String getNextSequenceValueSql(String seqname);
}
