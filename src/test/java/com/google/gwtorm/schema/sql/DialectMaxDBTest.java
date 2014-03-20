// Copyright 2014 Google Inc.
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

import com.google.gwtorm.data.Address;
import com.google.gwtorm.data.Person;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.PhoneBookDb2;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.jdbc.JdbcSchema;
import com.google.gwtorm.jdbc.SimpleDataSource;
import com.google.gwtorm.server.OrmDuplicateKeyException;
import com.google.gwtorm.server.OrmException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

public class DialectMaxDBTest {
  private static final String MAXDB_URL_KEY = "maxdb.url";
  private static final String MAXDB_USER_KEY = "maxdb.user";
  private static final String MAXDB_PASSWORD_KEY = "maxdb.password";
  private static final String MAXDB_DRIVER = "com.sap.dbtech.jdbc.DriverSapDB";
  private Connection db;
  private JdbcExecutor executor;
  private SqlDialect dialect;
  private Database<PhoneBookDb> phoneBook;
  private Database<PhoneBookDb2> phoneBook2;

  @Before
  public void setUp() throws Exception {
    try {
      Class.forName(MAXDB_DRIVER);
    } catch (Exception e) {
      assumeNoException(e);
    }

    final String url = System.getProperty(MAXDB_URL_KEY);
    final String user = System.getProperty(MAXDB_USER_KEY);
    final String pass = System.getProperty(MAXDB_PASSWORD_KEY);

    db = DriverManager.getConnection(url, user, pass);
    executor = new JdbcExecutor(db);
    dialect = new DialectMaxDB().refine(db);

    final Properties p = new Properties();
    p.setProperty("driver", MAXDB_DRIVER);
    p.setProperty("url", db.getMetaData().getURL());
    p.setProperty("user", user);
    p.setProperty("password", pass);
    phoneBook =
        new Database<PhoneBookDb>(new SimpleDataSource(p), PhoneBookDb.class);
    phoneBook2 =
        new Database<PhoneBookDb2>(new SimpleDataSource(p), PhoneBookDb2.class);

    drop("SEQUENCE address_id");
    drop("SEQUENCE cnt");

    drop("TABLE addresses");
    drop("TABLE foo");
    drop("TABLE bar");
    drop("TABLE people");
  }

  private void drop(String drop) {
    try {
      execute("DROP " + drop);
    } catch (OrmException e) {
    }
  }

  @After
  public void tearDown() {
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
  }

  @Test
  public void testUpgradeSchema() throws SQLException, OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      execute("CREATE SEQUENCE cnt");
      execute("CREATE TABLE foo (cnt INT)");

      execute("ALTER TABLE people ADD fake_name VARCHAR(20)");
      execute("ALTER TABLE people DROP COLUMN registered");
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
      final Person bob = new Person(pk, p.nextAddressId());
      p.people().insert(asList(bob));

      final Address addr =
          new Address(new Address.Key(pk, "home"), "some place");
      p.addresses().insert(asList(addr));
    } finally {
      p.close();
    }

    final PhoneBookDb2 p2 = phoneBook2.open();
    try {
      ((JdbcSchema) p2).renameField(executor, "people", "registered",
          "isRegistered");
    } finally {
      p2.close();
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

  @Test
  public void testInsert() throws OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      final Person.Key pk = new Person.Key("Bob");
      final Person bob = new Person(pk, p.nextAddressId());
      p.people().insert(asList(bob));

      try {
        p.people().insert(asList(bob));
        fail();
      } catch (OrmDuplicateKeyException duprec) {
        // expected
      }
    } finally {
      p.close();
    }
  }

  @Test
  public void testConstraintViolationOnIndex() throws OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      execute("CREATE UNIQUE INDEX idx ON people (age)");
      try {

        final Person.Key pk = new Person.Key("Bob");
        final Person bob = new Person(pk, p.nextAddressId());
        bob.setAge(40);
        p.people().insert(asList(bob));

        final Person.Key joePk = new Person.Key("Joe");
        Person joe = new Person(joePk, p.nextAddressId());
        joe.setAge(40);
        try {
        p.people().insert(asList(joe));
        fail();
        } catch (OrmDuplicateKeyException duprec) {
          fail();
        } catch (OrmException noDuprec) {
          // expeceted
        }
      } finally {
        execute("DROP INDEX idx ON people");
      }
    } finally {
      p.close();
    }
  }

  @Test
  public void testUpdate() throws OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      final Person.Key pk = new Person.Key("Bob");
      Person bob = new Person(pk, p.nextAddressId());
      bob.setAge(40);
      p.people().insert(asList(bob));

      bob.setAge(50);
      p.people().update(asList(bob));

      bob = p.people().get(pk);
      assertEquals(50, bob.age());
    } finally {
      p.close();
    }
  }

  @Test
  public void testUpsert() throws OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      final Person.Key bobPk = new Person.Key("Bob");
      Person bob = new Person(bobPk, p.nextAddressId());
      bob.setAge(40);
      p.people().insert(asList(bob));

      final Person.Key joePk = new Person.Key("Joe");
      Person joe = new Person(joePk, p.nextAddressId());
      bob.setAge(50);
      p.people().upsert(asList(bob, joe));

      bob = p.people().get(bobPk);
      assertEquals(50, bob.age());
      assertNotNull(p.people().get(joePk));
    } finally {
      p.close();
    }
  }

}
