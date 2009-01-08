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

import com.google.gwtorm.jdbc.gen.CodeGenSupport;
import com.google.gwtorm.schema.ColumnModel;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

public class SqlTimestampTypeInfo extends SqlTypeInfo {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  public static void setAsUTC(final PreparedStatement ps, final int col,
      final Timestamp val) throws SQLException {
    ps.setTimestamp(col, val, Calendar.getInstance(UTC));
  }

  public static Timestamp getAsUTC(final ResultSet rs, final int col)
      throws SQLException {
    Timestamp s = rs.getTimestamp(col);
    if (s != null) {
      final int o = s.getTimezoneOffset();
      if (o != 0) {
        s = new Timestamp(s.getTime() + (o * 60 * 1000L));
      }
    }
    return s;
  }

  @Override
  protected String getJavaSqlTypeAlias() {
    return "Timestamp";
  }

  @Override
  protected int getSqlTypeConstant() {
    return Types.TIMESTAMP;
  }

  @Override
  public void generateResultSetGet(CodeGenSupport cgs) {
    final Type typeCalendar = Type.getType(java.util.Calendar.class);
    cgs.fieldSetBegin();
    cgs.pushSqlHandle();
    cgs.pushColumnIndex();
    cgs.mv.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getType(
        SqlTimestampTypeInfo.class).getInternalName(), "getAsUTC", Type
        .getMethodDescriptor(Type.getType(Timestamp.class), new Type[] {
            Type.getType(ResultSet.class), Type.INT_TYPE}));
    cgs.fieldSetEnd();
  }

  @Override
  public void generatePreparedStatementSet(final CodeGenSupport cgs) {
    cgs.pushSqlHandle();
    cgs.pushColumnIndex();
    cgs.pushFieldValue();
    cgs.mv.visitMethodInsn(Opcodes.INVOKESTATIC, Type.getType(
        SqlTimestampTypeInfo.class).getInternalName(), "setAsUTC", Type
        .getMethodDescriptor(Type.VOID_TYPE, new Type[] {
            Type.getType(PreparedStatement.class), Type.INT_TYPE,
            Type.getType(Timestamp.class)}));
  }

  @Override
  public String getSqlType(final ColumnModel col, final SqlDialect dialect) {
    final StringBuilder r = new StringBuilder();
    r.append(dialect.getSqlTypeName(getSqlTypeConstant()));
    if (col.isNotNull()) {
      r.append(" DEFAULT '1900-01-01 00:00:00'");
      r.append(" NOT NULL");
    }
    return r.toString();
  }
}
