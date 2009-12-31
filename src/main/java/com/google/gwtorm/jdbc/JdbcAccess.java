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
import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.impl.AbstractAccess;
import com.google.gwtorm.client.impl.ListResultSet;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/** Internal base class for implementations of {@link Access}. */
public abstract class JdbcAccess<T, K extends Key<?>> extends
    AbstractAccess<T, K, JdbcTransaction> {
  private final JdbcSchema schema;

  protected JdbcAccess(final JdbcSchema s) {
    schema = s;
  }

  @Override
  public final com.google.gwtorm.client.ResultSet<T> get(final Iterable<K> keys)
      throws OrmException {
    final Collection<K> keySet;
    if (keys instanceof Collection) {
      keySet = (Collection<K>) keys;
    } else {
      keySet = new ArrayList<K>();
      for (final K k : keys) {
        keySet.add(k);
      }
    }

    switch (keySet.size()) {
      case 0:
        // Nothing requested, nothing to return.
        //
        return new ListResultSet<T>(Collections.<T> emptyList());

      case 1: {
        // Only one key requested, use a faster equality lookup.
        //
        final T entity = get(keySet.iterator().next());
        if (entity != null) {
          return new ListResultSet<T>(Collections.singletonList(entity));
        }
        return new ListResultSet<T>(Collections.<T> emptyList());
      }

      default:
        return getBySqlIn(keySet);
    }
  }

  protected com.google.gwtorm.client.ResultSet<T> getBySqlIn(
      final Collection<K> keys) throws OrmException {
    return super.get(keys);
  }

  protected PreparedStatement prepareStatement(final String sql)
      throws OrmException {
    try {
      return schema.getConnection().prepareStatement(sql);
    } catch (SQLException e) {
      throw convertError("prepare SQL\n" + sql + "\n", e);
    }
  }

  protected PreparedStatement prepareBySqlIn(final String sql,
      final Collection<K> keys) throws OrmException {
    final int n = keys.size();
    final StringBuilder buf = new StringBuilder(sql.length() + n << 1 + 1);
    buf.append(sql);
    buf.append('(');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        buf.append(',');
      }
      buf.append('?');
    }
    buf.append(')');
    return prepareStatement(buf.toString());
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
      throw convertError("fetch", e);
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
      throw convertError("fetch", e);
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
      throw convertError("insert", e);
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
      throw convertError("update", e);
    }
  }

  @Override
  protected void doUpsert(final Iterable<T> instances, final JdbcTransaction txn)
      throws OrmException {
    // Assume update first, it will cheaply tell us if the row is missing.
    //
    Collection<T> inserts = null;
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

        final int[] states = ps.executeBatch();
        if (states == null) {
          inserts = new ArrayList<T>(cnt);
          for(T o : instances){
            inserts.add(o);
          }
        } else {
          int i = 0;
          for (T o : instances) {
            if (states.length <= i || states[i] != 1) {
              if (inserts == null) {
                inserts = new ArrayList<T>(cnt - i);
              }
              inserts.add(o);
            }
            i++;
          }
        }

      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw convertError("update", e);
    }

    if (inserts != null) {
      doInsert(inserts, txn);
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
      throw convertError("delete", e);
    }
  }

  private static void execute(final PreparedStatement ps, final int cnt)
      throws SQLException, OrmConcurrencyException {
    if (cnt == 0) {
      return;
    }

    final int[] states = ps.executeBatch();
    if (states == null) {
      throw new SQLException("No rows affected; expected " + cnt + " rows");
    }
    for (int i = 0; i < cnt; i++) {
      if (states.length <= i || states[i] != 1) {
        throw new OrmConcurrencyException();
      }
    }
  }

  private OrmException convertError(final String op, final SQLException err) {
    if (err.getCause() == null && err.getNextException() != null) {
      err.initCause(err.getNextException());
    }
    return schema.getDialect().convertError(op, getRelationName(), err);
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
