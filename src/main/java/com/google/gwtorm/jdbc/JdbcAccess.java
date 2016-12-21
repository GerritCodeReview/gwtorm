// Copyright (C) 2008 The Android Open Source Project
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

import com.google.common.base.Preconditions;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.schema.sql.DialectDB2;
import com.google.gwtorm.server.AbstractAccess;
import com.google.gwtorm.server.Access;
import com.google.gwtorm.server.ListResultSet;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Internal base class for implementations of {@link Access}. */
public abstract class JdbcAccess<T, K extends Key<?>> extends
    AbstractAccess<T, K> {
  private final JdbcSchema schema;

  protected JdbcAccess(final JdbcSchema s) {
    schema = s;
  }

  @Override
  public void beginTransaction(K key) throws OrmException {
    try {
      schema.getConnection().setAutoCommit(false);
    } catch (SQLException e) {
      throw convertError("beginTransaction", e);
    }
  }

  @Override
  public final com.google.gwtorm.server.ResultSet<T> get(final Iterable<K> keys)
      throws OrmException {
    final Collection<K> keySet;
    if (keys instanceof Collection) {
      keySet = (Collection<K>) keys;
    } else {
      keySet = new ArrayList<>();
      for (final K k : keys) {
        keySet.add(k);
      }
    }

    switch (keySet.size()) {
      case 0:
        // Nothing requested, nothing to return.
        //
        return new ListResultSet<>(Collections.<T> emptyList());

      case 1: {
        // Only one key requested, use a faster equality lookup.
        //
        final T entity = get(keySet.iterator().next());
        if (entity != null) {
          return new ListResultSet<>(Collections.singletonList(entity));
        }
        return new ListResultSet<>(Collections.<T> emptyList());
      }

      default:
        return getBySqlIn(keySet);
    }
  }

  protected com.google.gwtorm.server.ResultSet<T> getBySqlIn(
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

  protected com.google.gwtorm.server.ResultSet<T> queryList(
      final PreparedStatement ps) throws OrmException {
    final ResultSet rs;
    try {
      rs = ps.executeQuery();
      if (!rs.next()) {
        rs.close();
        ps.close();
        return new ListResultSet<>(Collections.<T> emptyList());
      }
    } catch (SQLException err) {
      try {
        ps.close();
      } catch (SQLException e) {
        // Ignored.
      }
      throw convertError("fetch", err);
    }
    return new JdbcResultSet<>(this, rs, ps);
  }

  @Override
  public void insert(final Iterable<T> instances) throws OrmException {
    try {
      if (schema.getDialect().canDetermineTotalBatchUpdateCount()) {
        insertAsBatch(instances);
      } else {
        insertIndividually(instances);
      }
    } catch (SQLException e) {
      throwOrDefer(convertError("insert", e));
    } catch (OrmException e) {
      throwOrDefer(e);
    }
  }

  private void insertIndividually(Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      boolean concurrencyViolationDetected = false;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getInsertOneSql());
        }
        bindOneInsert(ps, o);
        int updateCount = ps.executeUpdate();
        if (updateCount != 1) {
          concurrencyViolationDetected = true;
        }
      }
      if (concurrencyViolationDetected) {
        throw new OrmConcurrencyException();
      }
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  private void insertAsBatch(final Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      int cnt = 0;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getInsertOneSql());
        }
        bindOneInsert(ps, o);
        ps.addBatch();
        cnt++;
      }
      execute(ps, cnt);
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  @Override
  public void update(final Iterable<T> instances) throws OrmException {
    try {
      if (schema.getDialect().canDetermineTotalBatchUpdateCount()) {
        updateAsBatch(instances);
      } else {
        updateIndividually(instances);
      }
    } catch (SQLException e) {
      throwOrDefer(convertError("update", e));
    } catch (OrmException e) {
      throwOrDefer(e);
    }
  }

  private void updateIndividually(Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      boolean concurrencyViolationDetected = false;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getUpdateOneSql());
        }
        bindOneUpdate(ps, o);
        int updateCount = ps.executeUpdate();
        if (updateCount != 1) {
          concurrencyViolationDetected = true;
        }
      }
      if (concurrencyViolationDetected) {
        throw new OrmConcurrencyException();
      }
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  private void updateAsBatch(final Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      int cnt = 0;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getUpdateOneSql());
        }
        bindOneUpdate(ps, o);
        ps.addBatch();
        cnt++;
      }
      execute(ps, cnt);
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  /**
   * Attempt to update instances.
   *
   * @param instances the instances to attempt to update
   * @return collection of instances that cannot be updated as they are not yet
   *         existing
   */
  private Collection<T> attemptUpdate(final Iterable<T> instances)
      throws OrmException {
    if (schema.getDialect().canDetermineIndividualBatchUpdateCounts()) {
      return attemptUpdateAsBatch(instances);
    } else {
      return attemptUpdatesIndividually(instances);
    }
  }

  private Collection<T> attemptUpdatesIndividually(Iterable<T> instances)
      throws OrmException {
    Collection<T> inserts = null;
    try {
      PreparedStatement ps = null;
      try {
        List<T> allInstances = new ArrayList<>();
        for (final T o : instances) {
          if (ps == null) {
            ps = schema.getConnection().prepareStatement(getUpdateOneSql());
          }
          bindOneUpdate(ps, o);
          int updateCount = ps.executeUpdate();
          if (updateCount != 1) {
            if (inserts == null) {
              inserts = new ArrayList<>();
            }
            inserts.add(o);          }
          allInstances.add(o);
        }
      } finally {
        if (ps != null) {
          ps.close();
        }
      }
    } catch (SQLException e) {
      throw convertError("update", e);
    }
    return inserts;
  }

  @Override
  public void upsert(final Iterable<T> instances) throws OrmException {
    try {
      // Assume update first, it will cheaply tell us if the row is missing.
      Collection<T> inserts = attemptUpdate(instances);

      if (inserts != null) {
        insert(inserts);
      }
    } catch (OrmException e) {
      throwOrDefer(e);
    }
  }

  private Collection<T> attemptUpdateAsBatch(final Iterable<T> instances)
      throws OrmException {
    Collection<T> inserts = null;
    try {
      PreparedStatement ps = null;
      try {
        int cnt = 0;
        List<T> allInstances = new ArrayList<>();
        for (final T o : instances) {
          if (ps == null) {
            ps = schema.getConnection().prepareStatement(getUpdateOneSql());
          }
          bindOneUpdate(ps, o);
          ps.addBatch();
          allInstances.add(o);
          cnt++;
        }

        if (0 < cnt) {
          Preconditions.checkNotNull(ps);
          final int[] states = ps.executeBatch();
          if (states == null) {
            inserts = allInstances;
          } else {
            int i = 0;
            for (T o : allInstances) {
              if (states.length <= i || states[i] != 1) {
                if (inserts == null) {
                  inserts = new ArrayList<>(cnt - i);
                }
                inserts.add(o);
              }
              i++;
            }
          }
        }
      } finally {
        if (ps != null) {
          ps.close();
        }
      }
    } catch (SQLException e) {
      throw convertError("update", e);
    }

    return inserts;
  }

  @Override
  public void delete(final Iterable<T> instances) throws OrmException {
    try {
      if (schema.getDialect().canDetermineTotalBatchUpdateCount()) {
        deleteAsBatch(instances);
      } else {
        deleteIndividually(instances);
      }
    } catch (SQLException e) {
      throwOrDefer(convertError("delete", e));
    } catch (OrmException e) {
      throwOrDefer(e);
    }
  }

  private void deleteIndividually(Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      boolean concurrencyViolationDetected = false;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getDeleteOneSql());
        }
        bindOneDelete(ps, o);
        int updateCount = ps.executeUpdate();
        if (updateCount != 1) {
          concurrencyViolationDetected = true;
        }
      }
      if (concurrencyViolationDetected) {
        throw new OrmConcurrencyException();
      }
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  private void deleteAsBatch(final Iterable<T> instances) throws SQLException,
      OrmConcurrencyException {
    PreparedStatement ps = null;
    try {
      int cnt = 0;
      for (final T o : instances) {
        if (ps == null) {
          ps = schema.getConnection().prepareStatement(getDeleteOneSql());
        }
        bindOneDelete(ps, o);
        ps.addBatch();
        cnt++;
      }
      execute(ps, cnt);
    } finally {
      if (ps != null) {
        ps.close();
      }
    }
  }

  private void execute(final PreparedStatement ps, final int cnt)
      throws SQLException, OrmConcurrencyException {
    if (cnt == 0) {
      return;
    }

    final int numberOfRowsUpdated = schema.getDialect().executeBatch(ps);
    if (numberOfRowsUpdated != cnt) {
        throw new OrmConcurrencyException();
    }
  }

  private void throwOrDefer(OrmException e) throws OrmException {
    try {
      if (!schema.isInTransaction()) {
        throw e;
      }
    } catch (SQLException e2) {
      // Couldn't determine if we were in a transaction. Assume we are not, and
      // just rethrow the original exception.
      throw e; // Not e2.
    }
    schema.setTransactionException(e);
  }

  protected OrmException convertError(final String op, SQLException err) {
    if (err.getCause() == null && err.getNextException() != null) {
      // special case for IBM DB2. Exception cause is null:
      // http://paste.openstack.org/show/193304/
      if (schema.getDialect() instanceof DialectDB2) {
        err = err.getNextException();
      } else {
        err.initCause(err.getNextException());
      }
    }
    return schema.getDialect().convertError(op, getRelationName(), err);
  }

  protected abstract T newEntityInstance();

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
