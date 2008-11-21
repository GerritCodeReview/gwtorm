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

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.impl.AbstractAccess;
import com.google.gwtorm.client.impl.ListResultSet;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/** Internal base class for implementations of {@link Access}. */
public abstract class JdbcAccess<T, K extends Key<?>> extends
    AbstractAccess<T, K, JdbcTransaction> {
  private final JdbcSchema schema;

  protected JdbcAccess(final JdbcSchema s) {
    schema = s;
  }

  protected PreparedStatement prepareStatement(final String sql)
      throws OrmException {
    try {
      return schema.getConnection().prepareStatement(sql);
    } catch (SQLException e) {
      throw new OrmException("Prepare failure\n" + sql, e);
    }
  }

  protected T queryOne(final PreparedStatement ps) throws OrmException {
    try {
      try {
        final ResultSet rs = ps.executeQuery();
        try {
          T r = null;
          if (rs.next()) {
            r = newEntityInstance();
            bindOneFetch(rs, r);
            if (rs.next()) {
              throw new OrmException("Multiple results");
            }
          }
          return r;
        } finally {
          rs.close();
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Fetch failure: " + getRelationName(), e);
    }
  }

  protected ListResultSet<T> queryList(final PreparedStatement ps)
      throws OrmException {
    try {
      try {
        final ResultSet rs = ps.executeQuery();
        try {
          final ArrayList<T> r = new ArrayList<T>();
          while (rs.next()) {
            final T o = newEntityInstance();
            bindOneFetch(rs, o);
            r.add(o);
          }
          return new ListResultSet<T>(r);
        } finally {
          rs.close();
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Fetch failure: " + getRelationName(), e);
    }
  }

  @Override
  protected void doInsert(final Iterable<T> instances, final JdbcTransaction txn)
      throws OrmException {
    try {
      final PreparedStatement ps;

      ps = schema.getConnection().prepareStatement(getInsertOneSql());
      try {
        int cnt = 0;
        for (final T o : instances) {
          bindOneInsert(ps, o);
          ps.addBatch();
          cnt++;
        }
        execute(ps, cnt);
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Insert failure: " + getRelationName(), e);
    }
  }

  @Override
  protected void doUpdate(final Iterable<T> instances, final JdbcTransaction txn)
      throws OrmException {
    try {
      final PreparedStatement ps;

      ps = schema.getConnection().prepareStatement(getUpdateOneSql());
      try {
        int cnt = 0;
        for (final T o : instances) {
          bindOneUpdate(ps, o);
          ps.addBatch();
          cnt++;
        }
        execute(ps, cnt);
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Update failure: " + getRelationName(), e);
    }
  }

  @Override
  protected void doDelete(final Iterable<T> instances, final JdbcTransaction txn)
      throws OrmException {
    try {
      final PreparedStatement ps;

      ps = schema.getConnection().prepareStatement(getDeleteOneSql());
      try {
        int cnt = 0;
        for (final T o : instances) {
          bindOneDelete(ps, o);
          ps.addBatch();
          cnt++;
        }
        execute(ps, cnt);
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Delete failure: " + getRelationName(), e);
    }
  }

  private static void execute(final PreparedStatement ps, final int cnt)
      throws SQLException {
    if (cnt == 0) {
      return;
    }

    final int[] states = ps.executeBatch();
    if (states == null) {
      throw new SQLException("No rows affected; expected " + cnt + " rows");
    }
    if (states.length != cnt) {
      throw new SQLException("Expected " + cnt + " rows affected, received "
          + states.length + " instead");
    }
    for (int i = 0; i < cnt; i++) {
      if (states[i] != 1) {
        throw new SQLException("Entity " + (i + 1) + " not affected by update");
      }
    }
  }

  protected abstract T newEntityInstance();

  protected abstract String getRelationName();

  protected abstract String getInsertOneSql();

  protected abstract String getUpdateOneSql();

  protected abstract String getDeleteOneSql();

  protected abstract void bindOneInsert(PreparedStatement ps, T entity)
      throws SQLException;

  protected abstract void bindOneUpdate(PreparedStatement ps, T entity)
      throws SQLException;

  protected abstract void bindOneDelete(PreparedStatement ps, T entity)
      throws SQLException;

  protected abstract void bindOneFetch(ResultSet rs, T entity)
      throws SQLException;
}
