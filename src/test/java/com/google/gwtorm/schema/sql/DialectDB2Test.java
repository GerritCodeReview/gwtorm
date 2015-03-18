// Copyright 2015 The Android Open Source Project
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

package com.google.gwtorm.schema.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import com.google.gwtorm.data.Address;
import com.google.gwtorm.data.Person;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.PhoneBookDb2;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.jdbc.JdbcSchema;
import com.google.gwtorm.jdbc.SimpleDataSource;
import com.google.gwtorm.server.OrmException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class DialectDB2Test extends SqlDialectTest {
  private final static String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
  @Before
  public void setUp() throws Exception {
    try {
      Class.forName(DB2_DRIVER);
    } catch (Exception e) {
      assumeNoException(e);
    }

    final String database = "GERRIT"; // database
    final String user = "gwtorm"; // user
    final String pass = "gwtorm"; // pwd

    db = DriverManager.getConnection("jdbc:db2://127.0.0.1:50001/"
        + database, user, pass);
    executor = new JdbcExecutor(db);
    dialect = new DialectDB2().refine(db);

    final Properties p = new Properties();
    p.setProperty("driver", DB2_DRIVER);
    p.setProperty("url", db.getMetaData().getURL());
    p.setProperty("user", user);
    p.setProperty("password", pass);
    phoneBook =
        new Database<>(new SimpleDataSource(p), PhoneBookDb.class);
    phoneBook2 =
        new Database<>(new SimpleDataSource(p), PhoneBookDb2.class);
  }

  @After
  public void tearDown() {
    if (executor == null) {
      return;
    }

    // Database content must be flushed because
    // tests assume that the database is empty
    drop("SEQUENCE address_id");
    drop("SEQUENCE cnt");

    drop("TABLE addresses");
    drop("TABLE foo");
    drop("TABLE bar");
    drop("TABLE people");

    if (executor != null) {
      executor.close();
    }
    executor = null;

    if (db != null) {
      try {
        db.close();
      } catch (SQLException e) {
        throw new RuntimeException("Cannot close database", e);
      }
    }
    db = null;
  }

  private void drop(String drop) {
    try {
      execute("DROP " + drop);
    } catch (OrmException e) {
    }
  }

  private void execute(final String sql) throws OrmException {
    executor.execute(sql);
  }

  @Test
  public void testListSequences() throws OrmException, SQLException {
    assertTrue(dialect.listSequences(db).isEmpty());

    execute("CREATE SEQUENCE cnt");
    execute("CREATE TABLE foo (cnt INT)");

    Set<String> s = dialect.listSequences(db);
    assertEquals(1, s.size());
    assertTrue(s.contains("cnt"));
    assertFalse(s.contains("foo"));
  }

  @Test
  public void testListTables() throws OrmException, SQLException {
    assertTrue(dialect.listTables(db).isEmpty());

    execute("CREATE SEQUENCE cnt");
    execute("CREATE TABLE foo (cnt INT)");

    Set<String> s = dialect.listTables(db);
    assertEquals(1, s.size());
    assertFalse(s.contains("cnt"));
    assertTrue(s.contains("foo"));
  }

  @Test
  public void testListIndexes() throws OrmException, SQLException {
    assertTrue(dialect.listTables(db).isEmpty());

    execute("CREATE SEQUENCE cnt");
    execute("CREATE TABLE foo (cnt INT, bar INT, baz INT)");
    execute("CREATE UNIQUE INDEX FOO_PRIMARY_IND ON foo(cnt)");
    execute("CREATE INDEX FOO_SECOND_IND ON foo(bar, baz)");

    Set<String> s = dialect.listIndexes(db, "foo");
    assertEquals(2, s.size());
    assertTrue(s.contains("foo_primary_ind"));
    assertTrue(s.contains("foo_second_ind"));

    dialect.dropIndex(executor, "foo", "foo_primary_ind");
    dialect.dropIndex(executor, "foo", "foo_second_ind");
    assertEquals(Collections.emptySet(), dialect.listIndexes(db, "foo"));
  }

  @Test
  public void testUpgradeSchema() throws SQLException, OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      execute("CREATE SEQUENCE cnt");
      execute("CREATE TABLE foo (cnt INT)");

      execute("ALTER TABLE people ADD fake_name VARCHAR(20)");
      execute("ALTER TABLE people DROP COLUMN fake_name");
      // This is needed because table is put in maintenance mode
      // when columns were dropped in DDL statement
      execute("call sysproc.admin_cmd ('reorg table people')");
      execute("DROP TABLE addresses");
      execute("DROP SEQUENCE address_id");

      Set<String> sequences, tables;

      p.updateSchema(executor);
      sequences = dialect.listSequences(db);
      tables = dialect.listTables(db);

      assertTrue(sequences.contains("cnt"));
      assertTrue(tables.contains("foo"));

      assertTrue(sequences.contains("address_id"));
      assertTrue(tables.contains("addresses"));

      p.pruneSchema(executor);
      sequences = dialect.listSequences(db);
      tables = dialect.listTables(db);
      assertFalse(sequences.contains("cnt"));
      assertFalse(tables.contains("foo"));

      final Person.Key pk = new Person.Key("Bob");
      int nextAddressId = p.nextAddressId();
      final Person bob = new Person(pk, nextAddressId);
      p.people().insert(Collections.singleton(bob));

      final Address addr =
          new Address(new Address.Key(pk, "home"), "some place");
      p.addresses().insert(Collections.singleton(addr));
    } finally {
      p.close();
    }
  }

  @Test
  public void testRenameTable() throws SQLException, OrmException {
    assertTrue(dialect.listTables(db).isEmpty());
    execute("CREATE TABLE foo (cnt INT)");
    Set<String> s = dialect.listTables(db);
    assertEquals(1, s.size());
    assertTrue(s.contains("foo"));
    final PhoneBookDb p = phoneBook.open();
    try {
      ((JdbcSchema) p).renameTable(executor, "foo", "bar");
    } finally {
      p.close();
    }
    s = dialect.listTables(db);
    assertTrue(s.contains("bar"));
    assertFalse(s.contains("for"));
  }
}
