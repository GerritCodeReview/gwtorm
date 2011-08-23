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

package com.google.gwtorm.nosql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gwtorm.data.Person;
import com.google.gwtorm.data.PersonAccess;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.nosql.heap.MemoryDatabase;
import com.google.gwtorm.server.Access;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NoSqlPhoneBookTest  {
  protected MemoryDatabase<PhoneBookDb> db;
  private List<PhoneBookDb> openSchemas;

  @Before
  public void setUp() throws Exception {
    db = new MemoryDatabase<PhoneBookDb>(PhoneBookDb.class);
    openSchemas = new ArrayList<PhoneBookDb>();
  }

  @After
  public void tearDown() throws Exception {
    if (openSchemas != null) {
      for (PhoneBookDb schema : openSchemas) {
        schema.close();
      }
      openSchemas = null;
    }
  }

  protected PhoneBookDb open() throws OrmException {
    final PhoneBookDb r = db.open();
    if (r != null) {
      openSchemas.add(r);
    }
    return r;
  }

  @Test
  public void testCreateDatabaseHandle() throws Exception {
    assertNotNull(db);
  }

  @Test
  public void testOpenSchema() throws Exception {
    final PhoneBookDb schema1 = open();
    assertNotNull(schema1);

    final PhoneBookDb schema2 = open();
    assertNotNull(schema2);
    assertNotSame(schema1, schema2);
  }

  @Test
  public void testGetPeopleAccess() throws Exception {
    final PhoneBookDb schema = open();
    assertNotNull(schema.people());
    assertEquals("people", schema.people().getRelationName());
    assertEquals(1, schema.people().getRelationID());
  }

  @Test
  public void testGetAddressAccess() throws Exception {
    final PhoneBookDb schema = open();
    assertNotNull(schema.addresses());
    assertEquals("addresses", schema.addresses().getRelationName());
    assertEquals(2, schema.addresses().getRelationID());
  }

  @Test
  public void testGetAllRelations() throws Exception {
    final PhoneBookDb schema = open();
    Access<?, ?>[] all = schema.allRelations();
    assertNotNull(all);
    assertEquals(2, all.length);
    assertSame(schema.people(), all[0]);
    assertSame(schema.addresses(), all[1]);
  }

  @Test
  public void testNextAddressId() throws Exception {
    final PhoneBookDb schema = open();
    final int a = schema.nextAddressId();
    final int b = schema.nextAddressId();
    assertTrue(a != b);
  }

  @Test
  public void testPersonPrimaryKey() throws Exception {
    final PhoneBookDb schema = open();
    final Person.Key key = new Person.Key("Bob");
    final Person bob = new Person(key, 18);
    assertSame(key, schema.people().primaryKey(bob));
  }

  @Test
  public void testInsertOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    Person copy = schema.people().all().toList().get(0);
    assertNotSame(copy, bob);
    assertEquals(bob.name(), copy.name());
  }

  @Test
  public void testGetOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final PersonAccess sp = schema.people();
    final Person p1 = new Person(new Person.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final Person p2 = sp.get(sp.primaryKey(p1));
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  @Test
  public void testGetAsyncOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final PersonAccess sp = schema.people();
    final Person p1 = new Person(new Person.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final Person p2 = sp.getAsync(sp.primaryKey(p1)).get();
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  @Test
  public void testGetOnePersonIterator() throws Exception {
    final PhoneBookDb schema = open();
    final PersonAccess sp = schema.people();
    final Person p1 = new Person(new Person.Key("Bob"), 18);
    sp.insert(Collections.singleton(p1));

    final List<Person> list =
        sp.get(Collections.singleton(sp.primaryKey(p1))).toList();
    assertNotNull(list);
    assertEquals(1, list.size());

    final Person p2 = list.get(0);
    assertNotNull(p2);
    assertNotSame(p1, p2);
    assertEquals(sp.primaryKey(p1), sp.primaryKey(p2));
  }

  @Test
  public void testInsertManyPeople() throws Exception {
    final PhoneBookDb schema = open();
    final ArrayList<Person> all = new ArrayList<Person>();
    all.add(new Person(new Person.Key("Bob"), 18));
    all.add(new Person(new Person.Key("Mary"), 22));
    all.add(new Person(new Person.Key("Zak"), 33));
    schema.people().insert(all);
  }

  @Test
  public void testDeleteOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));
    schema.people().delete(Collections.singleton(bob));
  }

  @Test
  public void testUpdateOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));
    bob.growOlder();
    schema.people().update(Collections.singleton(bob));
  }

  @Test
  public void testUpdateNoPerson() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    try {
      schema.people().update(Collections.singleton(bob));
      fail("Update of missing person succeeded");
    } catch (OrmConcurrencyException e) {
      assertEquals("Concurrent modification detected", e.getMessage());
    }
  }

  @Test
  public void testFetchOnePerson() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    final List<Person> all = schema.people().all().toList();
    assertNotNull(all);
    assertEquals(1, all.size());
    assertNotSame(bob, all.get(0));
    assertEquals(bob.name(), all.get(0).name());
    assertEquals(bob.age(), all.get(0).age());
    assertEquals(bob.isRegistered(), all.get(0).isRegistered());
  }

  @Test
  public void testFetchOnePersonByName() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob1 = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob1));

    final Person bob2 =
        schema.people().get(new Person.Key(bob1.name()));
    assertNotNull(bob2);
    assertNotSame(bob1, bob2);
    assertEquals(bob1.name(), bob2.name());
    assertEquals(bob1.age(), bob2.age());
    assertEquals(bob1.isRegistered(), bob2.isRegistered());
  }

  @Test
  public void testFetchByAge() throws Exception {
    final PhoneBookDb schema = open();
    final ArrayList<Person> all = new ArrayList<Person>();
    all.add(new Person(new Person.Key("Bob"), 18));
    all.add(new Person(new Person.Key("Mary"), 22));
    all.add(new Person(new Person.Key("Zak"), 33));
    schema.people().insert(all);

    final List<Person> r = schema.people().olderThan(20).toList();
    assertEquals(2, r.size());
    assertEquals(all.get(1).name(), r.get(0).name());
    assertEquals(all.get(2).name(), r.get(1).name());
  }

  @Test
  public void testBooleanType() throws Exception {
    final PhoneBookDb schema = open();
    final Person bob = new Person(new Person.Key("Bob"), 18);
    schema.people().insert(Collections.singleton(bob));

    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());

    bob.register();
    schema.people().update(Collections.singleton(bob));

    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());

    bob.unregister();
    schema.people().update(Collections.singleton(bob));

    assertEquals(bob.isRegistered(), schema.people().all().toList().get(0)
        .isRegistered());
  }
}
