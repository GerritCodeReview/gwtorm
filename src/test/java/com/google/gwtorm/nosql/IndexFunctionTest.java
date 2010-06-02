// Copyright 2010 Google Inc.
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

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.TestPerson;
import com.google.gwtorm.schema.QueryModel;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.server.GeneratedClassLoader;

import junit.framework.TestCase;

@SuppressWarnings("unchecked")
public class IndexFunctionTest extends TestCase {
  private JavaSchemaModel schema;
  private RelationModel people;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    schema = new JavaSchemaModel(PhoneBookDb.class);
    people = schema.getRelation("people");
  }

  public void testPersonByName() throws Exception {
    IndexFunction<TestPerson> idx = index("testMyQuery", "WHERE name=?");
    assertEquals("testMyQuery", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("bob"), 12);
    assertTrue(idx.includes(p));
    idx.encode(b, p);
    assertEquals(new byte[] {'b', 'o', 'b'}, b);
  }

  public void testPersonByNameAge() throws Exception {
    IndexFunction<TestPerson> idx = index("nameAge", "WHERE name=? AND age=?");
    assertEquals("nameAge", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("hm"), 42);
    assertTrue(idx.includes(p));
    idx.encode(b, p);
    assertEquals(new byte[] {'h', 'm', 0x00, 0x01, 0x01, 42}, b);

    p = new TestPerson(new TestPerson.Key(null), 0);
    assertFalse(idx.includes(p));

    b = new IndexKeyBuilder();
    assertFalse(idx.includes(p));
  }

  public void testPersonByNameAge_OrderByName() throws Exception {
    IndexFunction<TestPerson> idx =
        index("nameAge", "WHERE name=? AND age=? ORDER BY name");
    assertEquals("nameAge", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("qy"), 42);
    assertTrue(idx.includes(p));
    idx.encode(b, p);
    assertEquals(new byte[] {'q', 'y', 0x00, 0x01, 0x01, 42}, b);
  }

  public void testPersonByNameAge_OrderByRegistered() throws Exception {
    IndexFunction<TestPerson> idx =
        index("nameAge", "WHERE name=? AND age=? ORDER BY registered");
    assertEquals("nameAge", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("q"), 42);
    p.register();
    assertTrue(idx.includes(p));
    idx.encode(b, p);
    assertEquals(new byte[] {'q', 0x00, 0x01, // name
        0x01, 42, 0x00, 0x01, // age
        0x01, 0x01 // registered
        }, b);
  }

  public void testPersonByNameRange_OrderByName() throws Exception {
    IndexFunction<TestPerson> idx =
        index("nameSuggest", "WHERE name >= ? AND name <= ? ORDER BY name");
    assertEquals("nameSuggest", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("q"), 42);
    assertTrue(idx.includes(p));
    idx.encode(b, p);
    assertEquals(new byte[] {'q'}, b);
  }

  public void testOnlyRegistered() throws Exception {
    IndexFunction<TestPerson> idx =
        index("isregistered", "WHERE registered = true ORDER BY name");
    assertEquals("isregistered", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("q"), 42);
    assertFalse(idx.includes(p));
    p.register();
    assertTrue(idx.includes(p));

    idx.encode(b, p);
    assertEquals(new byte[] {'q'}, b);
  }

  public void testOnlyAge42() throws Exception {
    IndexFunction<TestPerson> idx =
        index("isOldEnough", "WHERE age = 42 ORDER BY name");
    assertEquals("isOldEnough", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("q"), 32);
    assertFalse(idx.includes(p));

    p = new TestPerson(new TestPerson.Key("q"), 42);
    assertTrue(idx.includes(p));

    idx.encode(b, p);
    assertEquals(new byte[] {'q'}, b);
  }

  public void testOnlyBob() throws Exception {
    IndexFunction<TestPerson> idx = index("isbob", "WHERE name.name = 'bob'");
    assertEquals("isbob", idx.getName());

    IndexKeyBuilder b;
    TestPerson p;

    b = new IndexKeyBuilder();
    p = new TestPerson(new TestPerson.Key("q"), 42);
    assertFalse(idx.includes(p));

    p = new TestPerson(new TestPerson.Key("bob"), 42);
    assertTrue(idx.includes(p));

    idx.encode(b, p);
    assertEquals(new byte[] {}, b);
  }

  private IndexFunction<TestPerson> index(String name, String query)
      throws OrmException {
    final QueryModel qm = new QueryModel(people, name, query);
    return new IndexFunctionGen(new GeneratedClassLoader(TestPerson.class
        .getClassLoader()), qm, TestPerson.class).create();
  }

  private static void assertEquals(byte[] exp, IndexKeyBuilder ic) {
    assertEquals(toString(exp), toString(ic.toByteArray()));
  }

  private static String toString(byte[] bin) {
    StringBuilder dst = new StringBuilder(bin.length * 2);
    for (byte b : bin) {
      dst.append(hexchar[(b >>> 4) & 0x0f]);
      dst.append(hexchar[b & 0x0f]);
    }
    return dst.toString();
  }

  private static final char[] hexchar =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', //
          'a', 'b', 'c', 'd', 'e', 'f'};
}
