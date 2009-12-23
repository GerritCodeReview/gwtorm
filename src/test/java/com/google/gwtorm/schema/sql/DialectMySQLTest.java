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

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.TestAddress;
import com.google.gwtorm.data.TestPerson;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.SimpleDataSource;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class DialectMySQLTest extends TestCase {
  private Connection db;
  private SqlDialect dialect;
  private Database<PhoneBookDb> phoneBook;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Class.forName(com.mysql.jdbc.Driver.class.getName());

    final String host = "localhost";
    final String database = "gwtorm";
    final String user = "gwtorm";
    final String pass = "gwtorm";

    final String url = "jdbc:mysql://" + host + "/" + database;
    db = DriverManager.getConnection(url, user, pass);
    dialect = new DialectMySQL().refine(db);

    final Properties p = new Properties();
    p.setProperty("driver", com.mysql.jdbc.Driver.class.getName());
    p.setProperty("url", db.getMetaData().getURL());
    p.setProperty("user", user);
    p.setProperty("password", pass);
    phoneBook =
        new Database<PhoneBookDb>(new SimpleDataSource(p), PhoneBookDb.class);

    drop("TABLE address_id");
    drop("TABLE addresses");
    drop("TABLE cnt");
    drop("TABLE foo");
    drop("TABLE people");
  }

  private void drop(String drop) {
    try {
      execute("DROP " + drop);
    } catch (SQLException e) {
    }
  }

  @Override
  protected void tearDown() {
    if (db != null) {
      try {
        db.close();
      } catch (SQLException e) {
        throw new RuntimeException("Cannot close database", e);
      }
    }
    db = null;
  }

  private void execute(final String sql) throws SQLException {
    final Statement stmt = db.createStatement();
    try {
      stmt.execute(sql);
    } finally {
      stmt.close();
    }
  }

  public void testListSequences() throws SQLException {
    assertTrue(dialect.listSequences(db).isEmpty());

    execute("CREATE TABLE cnt (s SERIAL)");
    execute("CREATE TABLE foo (cnt INT)");

    Set<String> s = dialect.listSequences(db);
    assertEquals(1, s.size());
    assertTrue(s.contains("cnt"));
    assertFalse(s.contains("foo"));
  }

  public void testListTables() throws SQLException {
    assertTrue(dialect.listTables(db).isEmpty());

    execute("CREATE TABLE cnt (s SERIAL)");
    execute("CREATE TABLE foo (cnt INT)");

    Set<String> s = dialect.listTables(db);
    assertEquals(1, s.size());
    assertFalse(s.contains("cnt"));
    assertTrue(s.contains("foo"));
  }

  public void testUpgradeSchema() throws SQLException, OrmException {
    final PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema();

      execute("CREATE TABLE cnt (s SERIAL)");
      execute("CREATE TABLE foo (cnt INT)");

      execute("ALTER TABLE people ADD COLUMN fake_name VARCHAR(20)");
      execute("ALTER TABLE people DROP COLUMN registered");
      execute("DROP TABLE addresses");
      execute("DROP TABLE address_id");

      Set<String> sequences, tables;

      p.updateSchema();
      sequences = dialect.listSequences(db);
      tables = dialect.listTables(db);
      assertTrue(sequences.contains("cnt"));
      assertTrue(tables.contains("foo"));

      assertTrue(sequences.contains("address_id"));
      assertTrue(tables.contains("addresses"));

      p.pruneSchema();
      sequences = dialect.listSequences(db);
      tables = dialect.listTables(db);
      assertFalse(sequences.contains("cnt"));
      assertFalse(tables.contains("foo"));

      final TestPerson.Key pk = new TestPerson.Key("Bob");
      final TestPerson bob = new TestPerson(pk, p.nextAddressId());
      p.people().insert(Collections.singleton(bob));

      final TestAddress addr =
          new TestAddress(new TestAddress.Key(pk, "home"), "some place");
      p.addresses().insert(Collections.singleton(addr));
    } finally {
      p.close();
    }
  }
}
