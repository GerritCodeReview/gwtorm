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

import com.google.gwtorm.client.Key;
import com.google.gwtorm.server.AbstractResultSet;
import com.google.gwtorm.server.OrmRuntimeException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

class JdbcResultSet<T, K extends Key<?>> extends AbstractResultSet<T> {
  private final JdbcAccess<T, K> access;
  private final ResultSet rs;
  private final PreparedStatement ps;
  private Boolean haveRow;
  private boolean closed;

  JdbcResultSet(JdbcAccess<T, K> jdbcAccess, ResultSet rs, PreparedStatement ps) {
    this.access = jdbcAccess;
    this.rs = rs;
    this.ps = ps;
    this.haveRow = Boolean.TRUE;
  }

  @Override
  protected boolean hasNext() {
    if (closed) {
      return false;
    }

    if (haveRow == null) {
      try {
        if (rs.next()) {
          haveRow = Boolean.TRUE;
        } else {
          haveRow = Boolean.FALSE;
          close();
        }
      } catch (SQLException err) {
        close();
        throw new OrmRuntimeException(access.convertError("fetch", err));
      }
    }

    return haveRow;
  }

  @Override
  protected T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final T o = access.newEntityInstance();
    try {
      access.bindOneFetch(rs, o);
    } catch (SQLException err) {
      close();
      throw new OrmRuntimeException(access.convertError("fetch", err));
    }

    haveRow = null;
    hasNext();
    return o;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;

      try {
        rs.close();
      } catch (SQLException e) {
        // Ignore
      }

      try {
        ps.close();
      } catch (SQLException e) {
        // Ignore
      }
    }
  }
}
