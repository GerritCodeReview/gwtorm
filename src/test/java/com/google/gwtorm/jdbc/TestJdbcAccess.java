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


import static java.util.Arrays.asList;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.schema.sql.SqlDialect;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class TestJdbcAccess {

  private static final String INSERT = "insert";
  private static final String UPDATE = "update";
  private static final String DELETE = "delete";

  private static final SqlDialect DIALECT = mock(SqlDialect.class,
      CALLS_REAL_METHODS);

  private static final Iterable<Data> NO_DATA = asList();
  private static  Iterable<Data> ONE_ROW = asList(new Data(1));
  private static  Iterable<Data> TWO_ROWS = asList(new Data(1), new Data(2));
  private Connection conn;


  private PreparedStatement stubStatementWithUpdateCounts(String command,
      final int... updateCounts) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);

    // batching
    doNothing().when(ps).addBatch();
    when(ps.executeBatch()).thenReturn(updateCounts);

    // non-batching
    if (updateCounts != null && updateCounts.length > 0) {
      OngoingStubbing<Integer> stubber = when(ps.executeUpdate());
      for (int updateCount : updateCounts) {
        stubber = stubber.thenReturn(updateCount);
      }
    }

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

  private JdbcAccess<Data, Data.DataKey> createJdbcAccess(
      final SqlDialect dialect) {
    JdbcSchema schema = setupSchema(dialect);

    JdbcAccess<Data, Data.DataKey> classUnderTest = new DataJdbcAccess(schema);
    return classUnderTest;
  }

  private JdbcAccess<Data, Data.DataKey> createClassUnderTest() {
    return createJdbcAccess(DIALECT);
  }

  private JdbcSchema setupSchema(final SqlDialect dialect) {
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

  private static void assertNotUsed(PreparedStatement insert) {
    verifyZeroInteractions(insert);
  }

  private static void assertUsedBatchingOnly(PreparedStatement statement)
      throws SQLException {
    verify(statement, atLeastOnce()).addBatch();
    verify(statement).executeBatch();
    verify(statement, never()).executeUpdate();
  }

  private static void assertExpectedIdsUsed(PreparedStatement statement,
      int... ids) throws SQLException {

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

  @Before
  public void setup() {
    conn = mock(Connection.class);
  }

  @Test
  public void testInsertNothing() throws OrmException {
    setup();
    createClassUnderTest().insert(NO_DATA);
  }

  @Test
  public void testInsertOne() throws OrmException, SQLException {
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    createClassUnderTest().insert(ONE_ROW);

    assertUsedBatchingOnly(insert);
  }

  @Test
  public void testInsertOneDBException() throws OrmException, SQLException {
    SQLException exception = new BatchUpdateException();
    PreparedStatement insert =
        stubStatementThrowExceptionOnExecute(INSERT, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.insert(ONE_ROW);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }

    assertUsedBatchingOnly(insert);
  }

  @Test
  public void testUpdateNothing() throws OrmException {
    createClassUnderTest().update(NO_DATA);
  }

  @Test
  public void testUpdateOne() throws OrmException, SQLException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1);

    createClassUnderTest().update(ONE_ROW);

    assertUsedBatchingOnly(update);
  }

  @Test
  public void testUpdateOneDBException() throws OrmException, SQLException {
    SQLException exception = new BatchUpdateException();
    PreparedStatement update =
        stubStatementThrowExceptionOnExecute(UPDATE, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.update(ONE_ROW);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }

    assertUsedBatchingOnly(update);
  }

  @Test
  public void testUpdateOneConcurrentlyModifiedException() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.update(ONE_ROW);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }
    assertUsedBatchingOnly(update);
  }

  @Test
  public void testUpsertNothing() throws OrmException, SQLException {
    createClassUnderTest().upsert(NO_DATA);
  }

  @Test
  public void testUpsertOneExisting() throws OrmException, SQLException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT);

    createClassUnderTest().upsert(ONE_ROW);

    assertUsedBatchingOnly(update);
    assertNotUsed(insert);
  }

  @Test
  public void testUpsertOneException() throws OrmException, SQLException {
    SQLException exception = new BatchUpdateException();
    PreparedStatement update =
        stubStatementThrowExceptionOnExecute(UPDATE, exception);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.upsert(ONE_ROW);
      fail("missingException");
    } catch (OrmException e) {
      // expected
      assertSame(e.getCause(), exception);
    }

    assertUsedBatchingOnly(update);
  }

  @Test
  public void testUpsertOneNotExisting() throws OrmException, SQLException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    createClassUnderTest().upsert(ONE_ROW);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 1);
  }

  @Test
  public void testUpsertTwoNotExistingZeroLengthArray() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoNotExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0, 0);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoBothExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertNotUsed(insert);
  }

  @Test
  public void testUpsertTwoFirstExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 1, 0);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 2);
  }

  @Test
  public void testUpsertTwoSecondExisting() throws SQLException, OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, 0, 1);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 1);
  }

  @Test
  public void testUpsertTwoUpdateCountsAreNull() throws SQLException,
      OrmException {
    PreparedStatement update = stubStatementWithUpdateCounts(UPDATE, null);
    PreparedStatement insert = stubStatementWithUpdateCounts(INSERT, 1, 1);

    createClassUnderTest().upsert(TWO_ROWS);

    assertUsedBatchingOnly(update);
    assertUsedBatchingOnly(insert);
    assertExpectedIdsUsed(insert, 1, 2);
  }

  @Test
  public void testDeleteOneExisting() throws SQLException, OrmException {
    PreparedStatement delete = stubStatementWithUpdateCounts(DELETE, 1);

    createClassUnderTest().delete(ONE_ROW);

    assertUsedBatchingOnly(delete);
  }

  @Test
  public void testDeleteOneNotExisting() throws SQLException, OrmException {
    PreparedStatement delete = stubStatementWithUpdateCounts(DELETE, 0);
    JdbcAccess<Data, Data.DataKey> classUnderTest = createClassUnderTest();
    try {
      classUnderTest.delete(ONE_ROW);
      fail("missing OrmConcurrencyException");
    } catch (OrmConcurrencyException e) {
      // expected
    }

    assertUsedBatchingOnly(delete);
  }

  private class Schema extends JdbcSchema {

    protected Schema(Database<?> d) throws OrmException {
      super(d);
    }

    @Override
    public Access<?, ?>[] allRelations() {
      throw new UnsupportedOperationException();
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
