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

import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.schema.sql.DialectH2;

import org.junit.Test;

public class PrintCreateTablesTestCase {

  @Test
  public void testCreate() throws Exception {
    final JavaSchemaModel m = new JavaSchemaModel(PhoneBookDb.class);
    System.out.println(m.getCreateDatabaseSql(new DialectH2()));
  }
}
