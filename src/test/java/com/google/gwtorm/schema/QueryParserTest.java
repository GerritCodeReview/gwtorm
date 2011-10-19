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

package com.google.gwtorm.schema;

import com.google.gwtorm.client.OrmException;

import junit.framework.TestCase;

import org.antlr.runtime.tree.Tree;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;

public class QueryParserTest extends TestCase {
  private final class DummyColumn extends ColumnModel {
    private String name;

    DummyColumn(final int id, final String n) {
      columnId = id;
      name = n;
      columnName = n;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public String getNestedClassName() {
      return null;
    }

    @Override
    public Class<?> getPrimitiveType() {
      return String.class;
    }

    @Override
    public Type[] getArgumentTypes() {
      return new Type[0];
    }

    @Override
    public boolean isCollection() {
      return false;
    }
  }

  protected Tree parse(final String str) throws QueryParseException {
    final RelationModel dummy = new RelationModel() {
      {
        final Collection<ColumnModel> c = new ArrayList<ColumnModel>();
        try {
          c.add(new DummyColumn(1, "name"));
          c.add(new DummyColumn(2, "a"));
          c.add(new DummyColumn(3, "b"));
          c.add(new DummyColumn(4, "c"));
          initColumns(c);
        } catch (OrmException e) {
          throw new RuntimeException("init columns failure", e);
        }
      }

      @Override
      public String getAccessInterfaceName() {
        return getClass().getName();
      }

      @Override
      public String getEntityTypeClassName() {
        return getClass().getName();
      }
    };
    return QueryParser.parse(dummy, str);
  }

  protected static void assertGoodEQ(final Tree c, final String fieldName) {
    assertEquals(QueryParser.EQ, c.getType());
    assertEquals(2, c.getChildCount());
    assertEquals(QueryParser.ID, c.getChild(0).getType());
    assertTrue(c.getChild(0) instanceof QueryParser.Column);
    assertEquals(fieldName, c.getChild(0).getText());
    assertEquals(QueryParser.PLACEHOLDER, c.getChild(1).getType());
  }

  public void testEmptyQuery() throws QueryParseException {
    assertNull(parse(""));
  }

  public void testWhereNameEq() throws QueryParseException {
    final Tree t = parse("WHERE name = ?");
    assertNotNull(t);
    assertEquals(QueryParser.WHERE, t.getType());

    assertEquals(1, t.getChildCount());
    assertGoodEQ(t.getChild(0), "name");
  }

  public void testWhereAAndBAndC() throws QueryParseException {
    final Tree t = parse("WHERE a = ? AND b = ? AND c = ?");
    assertNotNull(t);
    assertEquals(QueryParser.WHERE, t.getType());

    assertEquals(1, t.getChildCount());
    final Tree c = t.getChild(0);
    assertEquals(QueryParser.AND, c.getType());
    assertEquals(3, c.getChildCount());
    assertGoodEQ(c.getChild(0), "a");
    assertGoodEQ(c.getChild(1), "b");
    assertGoodEQ(c.getChild(2), "c");
  }

  public void testOrderByA() throws QueryParseException {
    final Tree t = parse("ORDER BY a");
    assertNotNull(t);
    assertEquals(QueryParser.ORDER, t.getType());
    assertEquals(1, t.getChildCount());

    final Tree a = t.getChild(0);
    assertEquals(QueryParser.ASC, a.getType());
    assertEquals(1, a.getChildCount());
    assertEquals(QueryParser.ID, a.getChild(0).getType());
    assertTrue(a.getChild(0) instanceof QueryParser.Column);
    assertEquals("a", a.getChild(0).getText());
  }

  public void testOrderByAB() throws QueryParseException {
    final Tree t = parse("ORDER BY a DESC, b ASC");
    assertNotNull(t);
    assertEquals(QueryParser.ORDER, t.getType());
    assertEquals(2, t.getChildCount());
    {
      final Tree a = t.getChild(0);
      assertEquals(QueryParser.DESC, a.getType());
      assertEquals(1, a.getChildCount());
      assertEquals(QueryParser.ID, a.getChild(0).getType());
      assertTrue(a.getChild(0) instanceof QueryParser.Column);
      assertEquals("a", a.getChild(0).getText());
    }
    {
      final Tree b = t.getChild(1);
      assertEquals(QueryParser.ASC, b.getType());
      assertEquals(1, b.getChildCount());
      assertEquals(QueryParser.ID, b.getChild(0).getType());
      assertTrue(b.getChild(0) instanceof QueryParser.Column);
      assertEquals("b", b.getChild(0).getText());
    }
  }

  public void testWhereAOrderByA() throws QueryParseException {
    final Tree t = parse("WHERE a = ? ORDER BY a");
    assertNotNull(t);
    assertEquals(0, t.getType());
    assertEquals(2, t.getChildCount());
    {
      final Tree w = t.getChild(0);
      assertEquals(QueryParser.WHERE, w.getType());
      assertEquals(1, w.getChildCount());
      assertGoodEQ(w.getChild(0), "a");
    }
    {
      final Tree o = t.getChild(1);
      assertEquals(QueryParser.ORDER, o.getType());
      assertEquals(1, o.getChildCount());

      final Tree a = o.getChild(0);
      assertEquals(QueryParser.ASC, a.getType());
      assertEquals(1, a.getChildCount());
      final Tree aId = a.getChild(0);
      assertEquals(QueryParser.ID, aId.getType());
      assertTrue(aId instanceof QueryParser.Column);
      assertEquals("a", aId.getText());
      assertEquals("a", ((QueryParser.Column) aId).getField().getFieldName());
    }
  }
}
