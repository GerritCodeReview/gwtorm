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


import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.gwtorm.client.Key;
import com.google.gwtorm.schema.sql.SqlDialect;
import com.google.gwtorm.server.Access;
import com.google.gwtorm.server.ListResultSet;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.ResultSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class AbstractTestJdbcAccess {

  private static final String INSERT = "insert";
  private static final String UPDATE = "update";
  private static final String DELETE = "delete";

  private final JdbcAccess<Data, Data.DataKey> classUnderTest;

  private final Iterable<Data> noData;
  private final Iterable<Data> oneRow;
  private final Iterable<Data> twoRows;
  private final Connection conn;
  protected final SqlDialect dialect;

  protected Integer totalUpdateCount = null;

  protected SQLException sqlException = null;

  static abstract class IterableProvider<T> {
    abstract Iterable<T> createIterable(T... ts);
  }

  private static final IterableProvider<Data> LIST_PROVIDER =
      new IterableProvider<Data>() {

        @Override
        Iterable<Data> createIterable(Data... data) {
          return Arrays.asList(data);
        }
      };

  private static final IterableProvider<Data> LIST_RESULT_SET_PROVIDER =
      new IterableProvider<Data>() {

        @Override
        Iterable<Data> createIterable(Data... data) {
          List<Data> list = Arrays.asList(data);
          return new ListResultSet<>(list);
        }
      };

  private static final IterableProvider<Data> UNMODIFIABLE_LIST_PROVIDER =
      new IterableProvider<Data>() {

        @Override
        Iterable<Data> createIterable(Data... data) {
          List<Data> list = Arrays.asList(data);
          return Collections.unmodifiableList(list);
        }
      };

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { {LIST_PROVIDER},
        {UNMODIFIABLE_LIST_PROVIDER}, {LIST_RESULT_SET_PROVIDER},});
  }

  public AbstractTestJdbcAccess(IterableProvider<Data> dataProvider)
      throws SQLException {
    noData = dataProvider.createIterable();
    oneRow = dataProvider.createIterable(new Data(1));
    twoRows = dataProvider.createIterable(new Data(1), new Data(2));
    conn = mock(Connection.class);
    dialect = createDialect();
    classUnderTest = createJdbcAccess(dialect, conn);
  }

  protected abstract SqlDialect createDialect() throws SQLException;

  private PreparedStatement stubStatementWithUpdateCounts(String command,
      final int... updateCounts) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);

    // batching
    doNothing().when(ps).addBatch();
    when(ps.executeBatch()).thenReturn(updateCounts);

    int total = 0;

    // non-batching
    if (updateCounts != null && updateCounts.length > 0) {
      OngoingStubbing<Integer> stubber = when(ps.executeUpdate());
      for (int updateCount : updateCounts) {
        stubber = stubber.thenReturn(updateCount);
        total += updateCount;
      }
    }

    totalUpdateCount = Integer.valueOf(total);

    when(conn.prepareStatement(command)).thenReturn(ps);
    return ps;
  }

  private PreparedStatement stubStatementThrowExceptionOnExecute(
      String command, SQLException exception) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doNothing().when(ps).addBatch();
    when(ps.executeBatch()).thenThrow(exception);
    when(ps.executeUpdate()).thenThrow(exception);
    when(conn.prepareStatement(command)).thenReturn(ps);
    return ps;
  }

  private static JdbcAccess<Data, Data.DataKey> createJdbcAccess(
      final SqlDialect dialect, Connection conn) {
    JdbcSchema schema = setupSchema(dialect, conn);

    JdbcAccess<Data, Data.DataKey> classUnderTest = new DataJdbcAccess(schema);
    return classUnderTest;
  }

  private static JdbcSchema setupSchema(final SqlDialect dialect,
      final Connection conn) {
    @SuppressWarnings("rawtypes")
    Database db = mock(Database.class);
    try {
      when(db.getDialect()).thenReturn(dialect);

      when(db.newConnection()).thenReturn(conn);

      JdbcSchema schema = new Schema(db);
      return schema;
    } catch (OrmException e) {
      throw new RuntimeException(e);
    }
  }

  protected static void assertUsedBatchingOnly(PreparedStatement ps, int... ids)
      throws SQLException {
    verify(ps, times(ids.length)).addBatch();
    verify(ps).executeBatch();
    verify(ps, never()).executeUpdate();
    assertExpectedIdsUsed(ps, ids);
  }

  protected static void assertUsedNonBatchingOnly(PreparedStatement ps,
      int... ids) throws SQLException {
    verify(ps, never()).addBatch();
    verify(ps, never()).executeBatch();
    verify(ps, times(ids.length)).executeUpdate();
    assertExpectedIdsUsed(ps, ids);
  }

  protected static void assertNotUsed(PreparedStatement insert) {
    verifyZeroInteractions(insert);
  }

  protected abstract void assertCorrectUpdating(PreparedStatement ps,
      int... ids) throws SQLException;

  protected abstract void assertCorrectAttempting(PreparedStatement ps,
      int... ids) throws SQLException;

  private static void assertExpectedIdsUsed(PreparedStatement ps, int... ids)
      throws SQLException {
    for (int id : ids) {
      verify(ps).setInt(1, id);
    }
  }

  @Test
  public void testInsertNothing() throws OrmException {
    classUnderTest.insert(noData);
  }

  @Test
  public void testInsertOne() throws SQLException, OrmException {
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    classUnderTest.insert(oneRow);

    assertCorrectUpdating(insert, 1);
  }

  @Test
  public void testInsertOneException() throws SQLException {
    sqlException = new BatchUpdateException();
    PreparedStatement insert =
        stubStatementThrowExceptionOnExecute(INSERT, sqlException);
    try {
      classUnderTest.insert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), sqlException);
    }

    assertCorrectUpdating(insert, 1);
  }

  @Test
  public void testUpdateNothing() throws OrmException {
    classUnderTest.update(noData);
  }

  @Test
  public void testUpdateOne() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1);

    classUnderTest.update(oneRow);

    assertCorrectUpdating(update, 1);
  }

  @Test
  public void testUpdateOneException() throws SQLException {
    sqlException = new BatchUpdateException();
    PreparedStatement update =
        stubStatementThrowExceptionOnExecute(UPDATE, sqlException);
    try {
      classUnderTest.update(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), sqlException);
    }

    assertCorrectUpdating(update, 1);
  }

  @Test
  public void testUpdateTwoConcurrentlyModifiedException() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0, 0);
    try {
      classUnderTest.update(twoRows);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
    assertCorrectUpdating(update, 1, 2);
  }

  @Test
  public void testUpsertNothing() throws OrmException {
    classUnderTest.upsert(noData);
  }

  @Test
  public void upsertOneExisting() throws OrmException, SQLException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT);

    classUnderTest.upsert(oneRow);

    assertCorrectAttempting(update, 1);
    assertNotUsed(insert);
  }

  @Test
  public void upsertOneException() throws SQLException {
    SQLException exception = new BatchUpdateException();
    PreparedStatement update =
        stubStatementThrowExceptionOnExecute(UPDATE, exception);
    try {
      classUnderTest.upsert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }

    assertCorrectAttempting(update, 1);
  }

  @Test
  public void upsertOneNotExisting() throws OrmException, SQLException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    classUnderTest.upsert(oneRow);

    assertCorrectAttempting(update, 1);
    assertCorrectUpdating(insert, 1);
  }

  @Test
  public void testUpsertTwoNotExistingZeroLengthArray() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertCorrectUpdating(insert, 1, 2);
  }

  @Test
  public void upsertTwoNotExistingNoInfo() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0, 0);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertCorrectUpdating(insert, 1, 2);
  }

  @Test
  public void upsertTwoBothExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertNotUsed(insert);
  }

  @Test
  public void upsertTwoFirstExistsingNoInfo() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1, 0);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertCorrectUpdating(insert, 2);
  }

  @Test
  public void testUpsertTwoUpdateCountsAreNull() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, null);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertCorrectUpdating(insert, 1, 2);
  }

  @Test
  public void upsertTwoSecondExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    classUnderTest.upsert(twoRows);

    assertCorrectAttempting(update, 1, 2);
    assertCorrectUpdating(insert, 1);
  }

  @Test
  public void deleteOneExisting() throws SQLException, OrmException {
    PreparedStatement delete = stubStatementWithUpdateCounts(DELETE, 1);

    classUnderTest.delete(oneRow);

    assertCorrectUpdating(delete, 1);
  }

  @Test
  public void deleteTwoNotExisting() throws SQLException, OrmException {
    PreparedStatement delete = stubStatementWithUpdateCounts(DELETE, 0, 1);
    try {
      classUnderTest.delete(twoRows);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }

    assertCorrectUpdating(delete, 1, 2);
  }

  private static class Schema extends JdbcSchema {

    protected Schema(Database<?> d) throws OrmException {
      super(d);
    }

    @Override
    public Access<?, ?>[] allRelations() {
      throw new UnsupportedOperationException();
    }

  }

  static class Data {

    private final int id;

    Data(int anId) {
      id = anId;
    }

    private static class DataKey implements Key<Key<?>> {

      @Override
      public com.google.gwtorm.client.Key<?> getParentKey() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fromString(String in) {
        throw new UnsupportedOperationException();
      }

    }

  }

  private static class DataJdbcAccess extends JdbcAccess<Data, Data.DataKey> {

    protected DataJdbcAccess(JdbcSchema s) {
      super(s);
    }

    @Override
    public String getRelationName() {
      return "Data";
    }

    @Override
    public Data.DataKey primaryKey(Data entity) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Data get(Data.DataKey key) throws OrmException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Data newEntityInstance() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String getInsertOneSql() {
      return INSERT;
    }

    @Override
    protected String getUpdateOneSql() {
      return UPDATE;
    }

    @Override
    protected String getDeleteOneSql() {
      return DELETE;
    }

    @Override
    protected void bindOneInsert(PreparedStatement ps, Data entity)
        throws SQLException {
      ps.setInt(1, entity.id);
    }

    @Override
    protected void bindOneUpdate(PreparedStatement ps, Data entity)
        throws SQLException {
      ps.setInt(1, entity.id);
    }

    @Override
    protected void bindOneDelete(PreparedStatement ps, Data entity)
        throws SQLException {
      ps.setInt(1, entity.id);
    }

    @Override
    protected void bindOneFetch(java.sql.ResultSet rs, Data entity)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getRelationID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet<Data> iterateAllEntities() throws OrmException {
      throw new UnsupportedOperationException();
    }

  }

}
