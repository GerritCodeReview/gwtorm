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
import static java.lang.Boolean.TRUE;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.impl.ListResultSet;
import com.google.gwtorm.schema.sql.SqlDialect;

import org.junit.Before;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestJdbcAccess {

  private static final String INSERT = "insert";
  private static final String UPDATE = "update";
  private static final String DELETE = "delete";

  private static final SqlDialect DIALECT = mock(SqlDialect.class,
      CALLS_REAL_METHODS);
  private static final SqlDialect NO_INFO_DIALECT = createNoInfoDialect();

  private final Iterable<Data> noData;
  private final Iterable<Data> oneRow;
  private final Iterable<Data> twoRows;
  private Connection conn;

  private static abstract class IterableProvider<T> {
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
          return new ListResultSet<TestJdbcAccess.Data>(list);
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

  public TestJdbcAccess(IterableProvider<Data> dataProvider) {
    noData = dataProvider.createIterable();
    oneRow = dataProvider.createIterable(new Data(1));
    twoRows = dataProvider.createIterable(new Data(1), new Data(2));
  }

  private static SqlDialect createNoInfoDialect() {
    final SqlDialect dialect = mock(SqlDialect.class);
    stub(dialect.canDetermineIndividualBatchUpdateCounts()).toReturn(FALSE);
    when(
        dialect.convertError(any(String.class), any(String.class),
            any(SQLException.class))).thenCallRealMethod();
    return dialect;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { LIST_PROVIDER },
        { UNMODIFIABLE_LIST_PROVIDER },
        { LIST_RESULT_SET_PROVIDER }});
  }

  private void stubPreparedStatementMethod(String command, PreparedStatement ps)
      throws SQLException {
    stub(conn.prepareStatement(command)).toReturn(ps);
  }

  private PreparedStatement stubExecuteBatchOK(String command,
      final int... updateCounts) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doNothing().when(ps).addBatch();
    stub(ps.executeBatch()).toReturn(updateCounts);
    stub(ps.executeUpdate()).toThrow(
        new AssertionError("unexpected method call"));
    stubPreparedStatementMethod(command, ps);
    return ps;
  }

  private PreparedStatement stubExecuteUpdateOK(String command,
      final int... updateCounts) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doThrow(new AssertionError("unexpected method call")).when(ps).addBatch();
    if (updateCounts.length > 0) {
      OngoingStubbing<Integer> stubber = when(ps.executeUpdate());
      for (int updateCount : updateCounts) {
        stubber = stubber.thenReturn(updateCount);
      }
    }
    stub(ps.executeBatch()).toThrow(
        new AssertionError("unexpected method call"));
    stubPreparedStatementMethod(command, ps);
    return ps;
  }

  private void stubExecuteBatchException(String command,
      BatchUpdateException exception) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doNothing().when(ps).addBatch();
    stub(ps.executeBatch()).toThrow(exception);
    stubPreparedStatementMethod(command, ps);
  }

  private void stubExecuteUpdateException(String command, SQLException e)
      throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    stub(ps.executeUpdate()).toThrow(e);
    stubPreparedStatementMethod(command, ps);
  }

  private JdbcAccess<Data, Data.DataKey> createClassUnderTest() {
    return createJdbcAccess(DIALECT);
  }

  private JdbcAccess<Data, Data.DataKey> createClassUnderTestNoInfo()
      throws SQLException {
    return createJdbcAccess(NO_INFO_DIALECT);
  }

  private JdbcAccess<Data, Data.DataKey> createJdbcAccess(
      final SqlDialect dialect) {
    JdbcSchema schema = setupSchema(dialect);

    JdbcAccess<Data, Data.DataKey> classUnderTest = new DataJdbcAccess(schema);
    return classUnderTest;
  }

  private JdbcAccess<Data, Data.DataKey> createClassUnderTestTotalCountOnly(int totalCount)
      throws SQLException {
    final SqlDialect dialect = mock(SqlDialect.class);
    stub(dialect.canDetermineIndividualBatchUpdateCounts()).toReturn(FALSE);
    stub(dialect.canDetermineTotalBatchUpdateCount()).toReturn(TRUE);
    stub(dialect.executeBatch(any(PreparedStatement.class))).toReturn(totalCount);
    when(
        dialect.convertError(any(String.class), any(String.class),
            any(SQLException.class))).thenCallRealMethod();
    JdbcSchema schema = setupSchema(dialect);

    JdbcAccess<Data, Data.DataKey> classUnderTest = new DataJdbcAccess(schema);
    return classUnderTest;
  }

  private JdbcSchema setupSchema(final SqlDialect dialect) {
    @SuppressWarnings("rawtypes")
    Database db = mock(Database.class);
    try {
      stub(db.getDialect()).toReturn(dialect);

      JdbcSchema schema = new Schema(db, conn);
      return schema;
    } catch (OrmException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup() {
    conn = mock(Connection.class);
  }

  @Test
  public void testInsertNothing() throws OrmException {
    setup();
    createClassUnderTest().insert(noData);
  }

  @Test
  public void testInsertOne() throws OrmException, SQLException {
    stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().insert(oneRow);
  }

  @Test
  public void testInsertNoInfoButTotalUpdateCount() throws OrmException, SQLException {
    int[] updateCounts = {SUCCESS_NO_INFO};
    stubExecuteBatchOK(INSERT, updateCounts);
    createClassUnderTestTotalCountOnly(1).insert(oneRow);
  }

  @Test
  public void testInsertNoInfo() throws OrmException, SQLException {
    stubExecuteUpdateOK(INSERT, 1);
    createClassUnderTestNoInfo().insert(oneRow);
  }

  @Test
  public void testInsertOneDBException() throws OrmException, SQLException {
    BatchUpdateException exception = new BatchUpdateException();
    stubExecuteBatchException(INSERT, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.insert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }
  }

  @Test
  public void testUpdateNothing() throws OrmException {
    createClassUnderTest().update(noData);
  }

  @Test
  public void testUpdateOne() throws OrmException, SQLException {
    stubExecuteBatchOK(UPDATE, 1);
    createClassUnderTest().update(oneRow);
  }

  @Test
  public void testUpdateOneDBException() throws OrmException, SQLException {
    BatchUpdateException exception = new BatchUpdateException();
    stubExecuteBatchException(UPDATE, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.update(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }
  }

  @Test
  public void testUpdateOneNoInfoException() throws OrmException, SQLException {
    SQLException exception = new SQLException();
    stubExecuteUpdateException(UPDATE, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest =
        createClassUnderTestNoInfo();
    try {
      classUnderTest.update(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      assertSame(e.getCause(), exception);
    }
  }

  @Test
  public void testUpdateOneNoInfo() throws OrmException, SQLException {
    stubExecuteUpdateOK(UPDATE, 1);
    createClassUnderTestNoInfo().update(oneRow);
  }

  @Test
  public void testUpdateOneConcurrentlyModifiedException() throws SQLException,
      OrmException {
    stubExecuteBatchOK(UPDATE, 0);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.update(oneRow);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
  }

  @Test
  public void testUpdateOneConcurrentlyModifiedExceptionNoInfo()
      throws SQLException, OrmException {
    stubExecuteUpdateOK(UPDATE, 0);
    JdbcAccess<Data, TestJdbcAccess.Data.DataKey> classUnderTest =
        createClassUnderTestNoInfo();
    try {
      classUnderTest.update(oneRow);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
  }

  @Test
  public void testUpsertNothing() throws OrmException, SQLException {
    createClassUnderTest().upsert(noData);
  }

  @Test
  public void testUpsertOneExisting() throws OrmException, SQLException {
    stubExecuteBatchOK(UPDATE, 1);
    PreparedStatement insert = stubExecuteBatchOK(INSERT);
    createClassUnderTest().upsert(oneRow);
    verifyZeroInteractions(insert);
  }

  @Test
  public void testUpsertOneExistingNoInfo() throws OrmException, SQLException {
    stubExecuteUpdateOK(UPDATE, 1);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT);
    createClassUnderTestNoInfo().upsert(oneRow);
    verifyZeroInteractions(insert);
  }

  @Test
  public void testUpsertOneException() throws OrmException, SQLException {
    BatchUpdateException exception = new BatchUpdateException();
    stubExecuteBatchException(UPDATE, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.upsert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }
  }

  @Test
  public void testUpsertOneNoInfoException() throws OrmException, SQLException {
    stubExecuteUpdateException(UPDATE, new SQLException());
    JdbcAccess<Data, Data.DataKey> classUnderTest =
        createClassUnderTestNoInfo();
    try {
      classUnderTest.upsert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
    }
  }

  @Test
  public void testUpsertOneNotExisting() throws OrmException, SQLException {
    stubExecuteBatchOK(UPDATE);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().upsert(oneRow);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertOneNotExistingNoInfo() throws OrmException,
      SQLException {
    stubExecuteUpdateOK(UPDATE, 0);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT, 1);
    createClassUnderTestNoInfo().upsert(oneRow);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertOneNotExistingNoInfoButTotalUpdateCount() throws OrmException,
      SQLException {
    stubExecuteUpdateOK(UPDATE, 0);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTestTotalCountOnly(1).upsert(oneRow);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertTwoNotExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoNotExistsingNoInfoButTotalUpdateCount() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 0, 0);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1, 1);
    createClassUnderTestTotalCountOnly(2).upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoNotExistsingNoInfo() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 0, 0);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT, 1, 1);
    createClassUnderTestNoInfo().upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoBothExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 1, 1);
    PreparedStatement insert = stubExecuteBatchOK(INSERT);
    createClassUnderTest().upsert(twoRows);
    verifyZeroInteractions(insert);
  }

  @Test
  public void testUpsertTwoBothExistsingNoInfo() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 1, 1);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT);
    createClassUnderTestNoInfo().upsert(twoRows);
    verifyZeroInteractions(insert);
  }

  @Test
  public void testUpsertTwoFirstExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 1, 0);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 2);
  }

  @Test
  public void testUpsertTwoFirstExistsingNoInfo() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 1, 0);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT, 1);
    createClassUnderTestNoInfo().upsert(twoRows);
    verifyIds(insert, 2);
  }

  @Test
  public void testUpsertTwoFirstExistsingNoInfoButTotalUpdateCount() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 1, 0);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTestTotalCountOnly(1).upsert(twoRows);
    verifyIds(insert, 2);
  }

  @Test
  public void testUpsertTwoSecondExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 0, 1);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertTwoUpdateCountsAreNull() throws SQLException,
      OrmException {
    stubExecuteBatchOK(UPDATE, null);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoSecondExistsingNoInfo() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 0, 1);
    PreparedStatement insert = stubExecuteUpdateOK(INSERT, 1);
    createClassUnderTestNoInfo().upsert(twoRows);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertTwoSecondExistsingNoInfoButTotalUpdateCount() throws SQLException,
      OrmException {
    stubExecuteUpdateOK(UPDATE, 0, 1);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTestTotalCountOnly(1).upsert(twoRows);
    verifyIds(insert, 1);
  }

  @Test
  public void testDeleteOneExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(DELETE, 1);
    createClassUnderTest().delete(oneRow);
  }

  @Test
  public void testDeleteOneExistingNoInfo() throws SQLException, OrmException {
    stubExecuteUpdateOK(DELETE, 1);
    createClassUnderTestNoInfo().delete(oneRow);
  }

  @Test
  public void testDeleteOneNotExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(DELETE, 0);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.delete(oneRow);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
  }

  @Test
  public void testDeleteOneDeletedNoInfo() throws SQLException, OrmException {
    stubExecuteUpdateOK(DELETE, 0);
    JdbcAccess<Data, Data.DataKey> classUnderTest =
        createClassUnderTestNoInfo();
    try {
      classUnderTest.delete(oneRow);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
  }

  private void verifyIds(PreparedStatement statement, int... ids)
      throws SQLException {
    Set<Integer> notSet = new HashSet<Integer>(2);
    notSet.add(1);
    notSet.add(2);

    for (int id : ids) {
      verify(statement).setInt(1, id);
      notSet.remove(Integer.valueOf(id));
    }

    for (Integer id : notSet) {
      verify(statement, never()).setInt(1, id);
    }
  }

  private class Schema extends JdbcSchema {

    protected Schema(Database<?> d, Connection conn) throws OrmException {
      super(d, conn);
    }

  }

  private static class Data {

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

    public String getRelationName() {
      return "Data";
    }

    @Override
    public com.google.gwtorm.jdbc.TestJdbcAccess.Data.DataKey primaryKey(
        Data entity) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Data get(com.google.gwtorm.jdbc.TestJdbcAccess.Data.DataKey key)
        throws OrmException {
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

  }
}
