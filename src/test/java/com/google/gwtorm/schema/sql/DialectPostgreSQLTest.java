// Copyright 2009 Google Inc.
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

import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.PhoneBookDb2;
import com.google.gwtorm.data.Address;
import com.google.gwtorm.data.Person;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.jdbc.JdbcSchema;
import com.google.gwtorm.jdbc.SimpleDataSource;
import com.google.gwtorm.server.OrmException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class DialectPostgreSQLTest extends SqlDialectTest {
  private Database<PhoneBookDb2> phoneBook2;

  @Before
  public void setUp() throws Exception {
    Class.forName(org.postgresql.Driver.class.getName());

    final String database = "gwtorm";
    final String user = "gwtorm";
    final String pass = "gwtorm";

    db = DriverManager.getConnection("jdbc:postgresql:" + database, user, pass);
    executor = new JdbcExecutor(db);
    dialect = new DialectPostgreSQL().refine(db);

    final Properties p = new Properties();
    p.setProperty("driver", org.postgresql.Driver.class.getName());
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
  public void testUpgradeSchema() throws SQLException, OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      execute("CREATE SEQUENCE cnt");
      execute("CREATE TABLE foo (cnt INT)");

      execute("ALTER TABLE people ADD COLUMN fake_name VARCHAR(20)");
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
      p.people().insert(Collections.singleton(bob));

      final Address addr =
          new Address(new Address.Key(pk, "home"), "some place");
      p.addresses().insert(Collections.singleton(addr));
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
}