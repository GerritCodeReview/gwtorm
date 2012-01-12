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

import static java.lang.Boolean.FALSE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gwtorm.schema.sql.SqlDialect;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@RunWith(Parameterized.class)
public class TestJdbcAccessNonBatching extends AbstractTestJdbcAccess {

  public TestJdbcAccessNonBatching(IterableProvider<Data> dataProvider)
      throws SQLException {
    super(dataProvider);
  }

  @Override
  protected void assertCorrectUpdating(PreparedStatement ps,
      int ... ids) throws SQLException {
    assertUsedNonBatchingOnly(ps, ids);
  }

  @Override
  protected SqlDialect createDialect() {
    final SqlDialect dialect = mock(SqlDialect.class);
    when(dialect.canDetermineIndividualBatchUpdateCounts()).thenReturn(FALSE);
    when(
        dialect.convertError(any(String.class), any(String.class),
            any(SQLException.class))).thenCallRealMethod();
    return dialect;
  }

}
