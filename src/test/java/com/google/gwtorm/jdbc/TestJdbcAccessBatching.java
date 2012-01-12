// Copyright (C) 2011 The Android Open Source Project
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

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import com.google.gwtorm.schema.sql.SqlDialect;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@RunWith(Parameterized.class)
public class TestJdbcAccessBatching extends AbstractTestJdbcAccess {

  public TestJdbcAccessBatching(IterableProvider<Data> dataProvider)
      throws SQLException {
    super(dataProvider);
  }

  @Override
  protected void assertCorrectUpdating(PreparedStatement ps,
      int ... ids) throws SQLException {
    assertUsedBatchingOnly(ps, ids);
  }

  @Override
  protected void assertCorrectAttempting(PreparedStatement ps,
      int ... ids) throws SQLException {
    assertUsedBatchingOnly(ps, ids);
  }

  @Override
  protected SqlDialect createDialect() {
    return mock(SqlDialect.class, CALLS_REAL_METHODS);
  }

}
