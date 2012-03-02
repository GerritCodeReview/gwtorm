// Copyright 2008 Google Inc.
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

package com.google.gwtorm.server;

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.OrmConcurrencyException;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.data.PersonAccess;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.TestPerson;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.jdbc.JdbcSchema;
import com.google.gwtorm.jdbc.SimpleDataSource;

import junit.framework.TestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class PhoneBookDbTestCase extends TestCase {
  private static int runCount;
  protected Database<PhoneBookDb> db;
  private List<PhoneBookDb> openSchemas;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    final Properties p = new Properties();
    p.setProperty("driver", org.h2.Driver.class.getName());
    p.setProperty("url", "jdbc:h2:mem:PhoneBookDb" + (runCount++));
    db = new Database<PhoneBookDb>(new SimpleDataSource(p), PhoneBookDb.class);
    openSchemas = new ArrayList<PhoneBookDb>();
  }

  @Override
  public void tearDown() throws Exception {
    if (openSchemas != null) {
      for (PhoneBookDb schema : openSchemas) {
        schema.close();
      }
      openSchemas = null;
    }
    super.tearDown();
  }

  protected PhoneBookDb open() throws OrmException {
    final PhoneBookDb r = db.open();
    if (r != null) {
      openSchemas.add(r);
    }
    return r;
  }

  protected PhoneBookDb openAndCreate() throws OrmException {
    final PhoneBookDb schema = open();
    final JdbcExecutor e = new JdbcExecutor((JdbcSchema) schema);
    try {
      schema.updateSchema(e);
    } finally {
      e.close();
    }
    return schema;
  }

  protected Statement statement(final PhoneBookDb schema) throws SQLException {
    return ((JdbcSchema) schema).getConnection().createStatement();
  }

  public void testCreateDatabaseHandle() throws Exception {
    assertNotNull(db);
  }

  public void testOpenSchema() throws Exception {
    final PhoneBookDb schema1 = open();
    assertNotNull(schema1);

    final PhoneBookDb schema2 = open();
    assertNotNull(schema2);
    assertNotSame(schema1, schema2);
  }

  public void testGetPeopleAccess() throws Exception {
    final PhoneBookDb schema = open();
    assertNotNull(schema.people());
    assertEquals("people", schema.people().getRelationName());
    assertEquals(1, schema.people().getRelationID());
  }

  public void testGetAddressAccess() throws Exception {
    final PhoneBookDb schema = open();
    assertNotNull(schema.addresses());
    assertEquals("addresses", schema.addresses().getRelationName());
    assertEquals(2, schema.addresses().getRelationID());
  }

  public void testGetAllRelations() throws Exception {
    final PhoneBookDb schema = open();
    Access<?, ?>[] all = schema.allRelations();
    assertNotNull(all);
    assertEquals(2, all.length);
    assertSame(schema.people(), all[0]);
    assertSame(schema.addresses(), all[1]);
  }

  public void testCreateSchema() throws Exception {
    final PhoneBookDb schema = open();
    final JdbcExecutor e = new JdbcExecutor((JdbcSchema) schema);
    try {
      schema.updateSchema(e);
    } finally {
      e.close();
    }
  }

  public void testNextAddressId() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final int a = schema.nextAddressId();
    final int b = schema.nextAddressId();
    assertTrue(a != b);
  }

  public void testPersonPrimaryKey() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson.Key key = new TestPerson.Key("Bob");
    final TestPerson bob = new TestPerson(key, 18);
    assertSame(key, schema.people().primaryKey(bob));
  }

  public void testInsertOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    final Statement st = statement(schema);
    final ResultSet rs = st.executeQuery("SELECT name,age FROM people");
    assertTrue(rs.next());
    assertEquals(bob.name(), rs.getString(1));
    assertEquals(bob.age(), rs.getInt(2));
    assertFalse(rs.next());
    rs.close();
    st.close();
  }

  public void testGetOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final PersonAccess sp = schema.people();
    final TestPerson p1 = new TestPerson(new TestPerson.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final TestPerson p2 = sp.get(sp.primaryKey(p1));
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  public void testGetAsyncOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final PersonAccess sp = schema.people();
    final TestPerson p1 = new TestPerson(new TestPerson.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final TestPerson p2 = sp.getAsync(sp.primaryKey(p1)).get();
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  public void testGetOnePersonIterator() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final PersonAccess sp = schema.people();
    final TestPerson p1 = new TestPerson(new TestPerson.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final List<TestPerson> list =
        sp.get(Collections.singleton(sp.primaryKey(p1))).toList();
    assertNotNull(list);
    assertEquals(1, list.size());

    final TestPerson p2 = list.get(0);
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  public void testInsertManyPeople() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final ArrayList<TestPerson> all = new ArrayList<TestPerson>();
    all.add(new TestPerson(new TestPerson.Key("Bob"), 18));
    all.add(new TestPerson(new TestPerson.Key("Mary"), 22));
    all.add(new TestPerson(new TestPerson.Key("Zak"), 33));
    schema.people().insert(all);

    final Statement st = statement(schema);
    final ResultSet rs;
    rs = st.executeQuery("SELECT name,age FROM people ORDER BY name");
    for (int rowIdx = 0; rowIdx < all.size(); rowIdx++) {
      assertTrue(rs.next());
      assertEquals(all.get(rowIdx).name(), rs.getString(1));
      assertEquals(all.get(rowIdx).age(), rs.getInt(2));
    }
    assertFalse(rs.next());
    rs.close();
    st.close();
  }

  public void testDeleteOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));
    schema.people().delete(Collections.singleton(bob));

    final Statement st = statement(schema);
    final ResultSet rs = st.executeQuery("SELECT name,age FROM people");
    assertFalse(rs.next());
    rs.close();
    st.close();
  }

  public void testUpdateOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));
    bob.growOlder();
    schema.people().update(Collections.singleton(bob));

    final Statement st = statement(schema);
    final ResultSet rs = st.executeQuery("SELECT name,age FROM people");
    assertTrue(rs.next());
    assertEquals(bob.name(), rs.getString(1));
    assertEquals(bob.age(), rs.getInt(2));
    assertFalse(rs.next());
    rs.close();
    st.close();
  }

  public void testUpdateNoPerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    try {
      schema.people().update(Collections.singleton(bob));
      fail("Update of missing person succeeded");
    } catch (OrmConcurrencyException e) {
      assertEquals("Concurrent modification detected", e.getMessage());
    }
  }

  public void testFetchOnePerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    final List<TestPerson> all = schema.people().all().toList();
    assertNotNull(all);
    assertEquals(1, all.size());
    assertNotSame(bob, all.get(0));
    assertEquals(bob.name(), all.get(0).name());
    assertEquals(bob.age(), all.get(0).age());
    assertEquals(bob.isRegistered(), all.get(0).isRegistered());
  }

  public void testFetchOnePersonByName() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob1 = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob1));

    final TestPerson bob2 =
        schema.people().get(new TestPerson.Key(bob1.name()));
    assertNotNull(bob2);
    assertNotSame(bob1, bob2);
    assertEquals(bob1.name(), bob2.name());
    assertEquals(bob1.age(), bob2.age());
    assertEquals(bob1.isRegistered(), bob2.isRegistered());
  }

  public void testFetchByAge() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final ArrayList<TestPerson> all = new ArrayList<TestPerson>();
    all.add(new TestPerson(new TestPerson.Key("Bob"), 18));
    all.add(new TestPerson(new TestPerson.Key("Mary"), 22));
    all.add(new TestPerson(new TestPerson.Key("Zak"), 33));
    schema.people().insert(all);

    final List<TestPerson> r = schema.people().olderThan(20).toList();
    assertEquals(2, r.size());
    assertEquals(all.get(1).name(), r.get(0).name());
    assertEquals(all.get(2).name(), r.get(1).name());
  }

  public void testFetchNotPerson() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final ArrayList<TestPerson> all = new ArrayList<TestPerson>();
    all.add(new TestPerson(new TestPerson.Key("Bob"), 18));
    all.add(new TestPerson(new TestPerson.Key("Mary"), 22));
    all.add(new TestPerson(new TestPerson.Key("Zak"), 33));
    schema.people().insert(all);

    final List<TestPerson> r =
        schema.people().olderThanDescByName(18).toList();
    assertEquals(2, r.size());
    assertEquals(all.get(2).name(), r.get(0).name());
    assertEquals(all.get(1).name(), r.get(1).name());
  }

  public void testBooleanType() throws Exception {
    final PhoneBookDb schema = openAndCreate();
    final TestPerson bob = new TestPerson(new TestPerson.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    final Statement st = statement(schema);
    ResultSet rs;

    rs = st.executeQuery("SELECT registered FROM people");
    assertTrue(rs.next());
    assertEquals("N", rs.getString(1));
    assertFalse(rs.next());
    rs.close();
    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());

    bob.register();
    schema.people().update(Collections.singleton(bob));
    rs = st.executeQuery("SELECT registered FROM people");
    assertTrue(rs.next());
    assertEquals("Y", rs.getString(1));
    assertFalse(rs.next());
    rs.close();
    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());

    bob.unregister();
    schema.people().update(Collections.singleton(bob));
    rs = st.executeQuery("SELECT registered FROM people");
    assertTrue(rs.next());
    assertEquals("N", rs.getString(1));
    assertFalse(rs.next());
    rs.close();
    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());

    st.close();
  }
}
