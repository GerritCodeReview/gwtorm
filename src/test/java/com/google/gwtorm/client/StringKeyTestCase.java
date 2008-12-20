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

package com.google.gwtorm.client;

import junit.framework.TestCase;


public class StringKeyTestCase extends TestCase {
  private abstract static class StringKeyImpl<T extends Key<?>> extends
      StringKey<T> {
    @Column
    String name;

    public StringKeyImpl(String n) {
      name = n;
    }

    @Override
    public String get() {
      return name;
    }
  }

  private static class Parent extends StringKeyImpl<Key<?>> {
    public Parent(String n) {
      super(n);
    }
  }

  private static class UnrelatedEntity extends StringKeyImpl<Key<?>> {
    public UnrelatedEntity(String n) {
      super(n);
    }
  }

  private static class Child extends StringKeyImpl<Parent> {
    private Parent parent;

    public Child(Parent p, String n) {
      super(n);
      parent = p;
    }

    @Override
    public Parent getParentKey() {
      return parent;
    }
  }

  public void testHashCodeWhenNull() {
    final Parent p = new Parent(null);
    assertEquals(0, p.hashCode());
  }

  public void testParentHashCode() {
    final String str = "foo";
    final Parent p = new Parent(str);
    assertEquals(str.hashCode(), p.hashCode());
  }

  public void testParentEquals() {
    final String str = "foo";
    final Parent p1 = new Parent(str);
    final Parent p2 = new Parent(str);
    assertTrue(p1.equals(p1));
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));
    assertFalse(p1.equals(null));

    final UnrelatedEntity u = new UnrelatedEntity(str);
    assertFalse(p1.equals(u));
    assertFalse(u.equals(p1));

    final Parent p3 = new Parent("bar");
    assertFalse(p1.equals(p3));
  }

  public void testChildHashCode() {
    final String pName = "foo";
    final String cName = "bar";
    final Parent p = new Parent(pName);
    final Child c = new Child(p, cName);
    assertSame(p, c.getParentKey());
    assertTrue(cName.hashCode() != c.hashCode());
  }

  public void testChildEquals() {
    final String pName = "foo";
    final String cName = "bar";
    final Child c1 = new Child(new Parent(pName), cName);
    final Child c2 = new Child(new Parent(pName), cName);
    assertTrue(c1.equals(c1));
    assertTrue(c1.equals(c2));
    assertTrue(c2.equals(c1));
    assertFalse(c1.equals(null));

    final UnrelatedEntity u = new UnrelatedEntity("bar");
    assertFalse(c1.equals(u));
    assertFalse(u.equals(c1));
  }
}
