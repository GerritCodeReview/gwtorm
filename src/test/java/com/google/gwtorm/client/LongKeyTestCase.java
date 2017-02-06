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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.gwtorm.server.StandardKeyEncoder;

import org.junit.Before;
import org.junit.Test;


public class LongKeyTestCase {
  @SuppressWarnings("serial")
  private abstract static class LongKeyImpl<T extends Key<?>> extends
      LongKey<T> {
    @Column(id = 1)
    long id;

    public LongKeyImpl(long n) {
      id = n;
    }

    @Override
    public long get() {
      return id;
    }

    @Override
    protected void set(long newValue) {
      id = newValue;
    }
  }

  @SuppressWarnings("serial")
  private static class Parent extends LongKeyImpl<Key<?>> {
    public Parent(long n) {
      super(n);
    }
  }

  @SuppressWarnings("serial")
  private static class UnrelatedEntity extends LongKeyImpl<Key<?>> {
    public UnrelatedEntity(long n) {
      super(n);
    }
  }

  @SuppressWarnings("serial")
  private static class Child extends LongKeyImpl<Parent> {
    private Parent parent;

    public Child(Parent p, long n) {
      super(n);
      parent = p;
    }

    @Override
    public Parent getParentKey() {
      return parent;
    }
  }

  @Before
  public void setUp() throws Exception {
    KeyUtil.setEncoderImpl(new StandardKeyEncoder());
  }

  @Test
  public void testHashCodeWhenNull() {
    final Parent p = new Parent(0);
    assertEquals(0, p.hashCode());
  }

  @Test
  public void testParentHashCode() {
    final long id = 21281821821821881L;
    final Parent p = new Parent(id);
    assertEquals((int) id, p.hashCode());
  }

  @Test
  public void testParentEquals() {
    final long id = 21281821821821881L;
    final Parent p1 = new Parent(id);
    final Parent p2 = new Parent(id);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));
    assertFalse(p1.equals(null));

    final UnrelatedEntity u = new UnrelatedEntity(id);
    assertFalse(p1.equals(u));
    assertFalse(u.equals(p1));

    final Parent p3 = new Parent(64);
    assertFalse(p1.equals(p3));
  }

  @Test
  public void testChildHashCode() {
    final long pId = 21281821821821881L;
    final long cId = 8;
    final Parent p = new Parent(pId);
    final Child c = new Child(p, cId);
    assertSame(p, c.getParentKey());
    assertTrue(cId != c.hashCode());
  }

  @Test
  public void testChildEquals() {
    final long pId = 21281821821821881L;
    final long cId = 8;
    final Child c1 = new Child(new Parent(pId), cId);
    final Child c2 = new Child(new Parent(pId), cId);
    assertTrue(c1.equals(c2));
    assertTrue(c2.equals(c1));
    assertFalse(c1.equals(null));

    final UnrelatedEntity u = new UnrelatedEntity(cId);
    assertFalse(c1.equals(u));
    assertFalse(u.equals(c1));
  }

  @Test
  public void testParentString() {
    final long pId = 21281821821821881L;
    final Parent p1 = new Parent(pId);
    assertEquals("" + pId, p1.toString());

    final Parent p2 = new Parent(0);
    p2.fromString(p1.toString());
    assertEquals(p1, p2);
  }

  @Test
  public void testChildString() {
    final long pId = 21281821821821881L;
    final long cId = 18218181281818888L;
    final Child c1 = new Child(new Parent(pId), cId);
    assertEquals(pId + "," + cId, c1.toString());

    final Child c2 = new Child(new Parent(0), 0);
    c2.fromString(c1.toString());
    assertEquals(c1, c2);
  }
}
