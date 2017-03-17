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

package com.google.gwtorm.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.schema.sql.DialectH2;
import com.google.gwtorm.server.OrmException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class QueryModelTest {

  private RelationModel people;

  @Before
  public void setUp() throws OrmException {
    JavaSchemaModel schema = new JavaSchemaModel(PhoneBookDb.class);
    people = schema.getRelation("people");
  }

  @Test
  public void testLimitWithConstant() throws OrmException {
    QueryModel qm = new QueryModel(people, null, "LIMIT 5");
    List<ColumnModel> params = qm.getParameters();
    assertNotNull(params);
    assertTrue(params.size() == 0);
    assertTrue(qm.hasLimit());

    String sql = qm.getSelectSql(new DialectH2(), "T");
    assertEquals("SELECT T.age,T.registered,T.name FROM people T LIMIT 5", sql);
  }

  @Test
  public void testLimitWithPlaceholder() throws OrmException {
    QueryModel qm = new QueryModel(people, null, "LIMIT ?");
    List<ColumnModel> params = qm.getParameters();
    assertNotNull(params);
    assertTrue(params.size() == 0);
    assertTrue(qm.hasLimit());

    String sql = qm.getSelectSql(new DialectH2(), "T");
    assertEquals("SELECT T.age,T.registered,T.name FROM people T LIMIT ?", sql);
  }
}
