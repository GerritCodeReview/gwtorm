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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

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

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { LIST_PROVIDER },
        { UNMODIFIABLE_LIST_PROVIDER },
        { LIST_RESULT_SET_PROVIDER }});
  }

  private PreparedStatement stubExecuteBatchOK(String command,
      final int... updateCounts) throws SQLException {
    PreparedStatement ps = setupPreparedStatementForBatch(updateCounts);
    stub(conn.prepareStatement(command)).toReturn(ps);
    return ps;
  }

  private void stubExecuteBatchException(String command, SQLException e)
      throws SQLException {
    PreparedStatement ps = setupPreparedStatementException(e);
    stub(conn.prepareStatement(command)).toReturn(ps);
  }

  private JdbcAccess<Data, Data.DataKey> createClassUnderTest() {
    final SqlDialect dialect = mock(SqlDialect.class, CALLS_REAL_METHODS);
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

  private PreparedStatement setupPreparedStatementForBatch(
      final int[] updateCounts) throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doNothing().when(ps).addBatch();
    stub(ps.executeBatch()).toReturn(updateCounts);
    stub(ps.executeUpdate()).toThrow(
        new AssertionError("unexpected method call"));
    return ps;
  }

  private PreparedStatement setupPreparedStatementException(SQLException e)
      throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    doNothing().when(ps).addBatch();
    stub(ps.executeBatch()).toThrow(e);
    stub(ps.executeUpdate()).toThrow(e);
    return ps;
  }

  @Before
  public void setup() {
    conn = mock(Connection.class);
  }

  @Test
  public void testInsertNothing() throws OrmException, SQLException {
    setup();
    createClassUnderTest().insert(noData);
  }

  @Test
  public void testInsertOne() throws OrmException, SQLException {
    stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().insert(oneRow);
  }

  @Test
  public void testInsertOneException() throws OrmException, SQLException {
    stubExecuteBatchException(INSERT, new BatchUpdateException());
    try {
      createClassUnderTest().insert(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
    }
  }

  @Test
  public void testUpdateNothing() throws OrmException, SQLException {
    createClassUnderTest().update(noData);
  }

  @Test
  public void testUpdateOne() throws OrmException, SQLException {
    stubExecuteBatchOK(UPDATE, 1);
    createClassUnderTest().update(oneRow);
  }

  @Test
  public void testUpdateOneException() throws OrmException, SQLException {
    stubExecuteBatchException(UPDATE, new BatchUpdateException());
    try {
      createClassUnderTest().update(oneRow);
      fail("missingException");
    } catch (OrmException e) {
      // expected
    }
  }

  @Test
  public void testUpdateOneModified() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 0);
    JdbcAccess<Data, TestJdbcAccess.Data.DataKey> classUnderTest =
        createClassUnderTest();
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
    createClassUnderTest().upsert(oneRow);
  }

  @Test
  public void testUpsertOneException() throws OrmException, SQLException {
    stubExecuteBatchException(UPDATE, new BatchUpdateException());
    try {
      createClassUnderTest().upsert(oneRow);
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
  public void testUpsertTwoNotExistsing() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  @Test
  public void testUpsertTwoBothExistsing() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 1, 1);
    createClassUnderTest().upsert(twoRows);
  }

  @Test
  public void testUpsertTwoFirstExistsing() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 1, 0);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 2);
  }

  @Test
  public void testUpsertTwoSecondExistsing() throws SQLException, OrmException {
    stubExecuteBatchOK(UPDATE, 0, 1);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1);
  }

  @Test
  public void testUpsertTwoUpdateCountsAreNull() throws SQLException, OrmException {
    stubExecuteBatchNull(UPDATE);
    PreparedStatement insert = stubExecuteBatchOK(INSERT, 1, 1);
    createClassUnderTest().upsert(twoRows);
    verifyIds(insert, 1, 2);
  }

  private PreparedStatement stubExecuteBatchNull(String command) throws SQLException {
    PreparedStatement ps = setupPreparedStatementForBatch(null);
    stub(conn.prepareStatement(command)).toReturn(ps);
    return ps;
  }


  @Test
  public void testDeleteOneExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(DELETE, 1);
    createClassUnderTest().delete(oneRow);
  }

  @Test
  public void testDeleteOneNotExisting() throws SQLException, OrmException {
    stubExecuteBatchOK(DELETE, 0);
    try {
      createClassUnderTest().delete(oneRow);
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
