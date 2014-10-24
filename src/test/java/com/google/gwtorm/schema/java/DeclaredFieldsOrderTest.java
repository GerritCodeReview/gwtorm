// Copyright (C) 2014 The Android Open Source Project
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

package com.google.gwtorm.schema.java;

import static com.google.gwtorm.schema.java.JavaColumnModel.sort;
import static com.google.common.collect.Collections2.permutations;

import static org.junit.Assert.assertEquals;

import com.google.gwtorm.client.Column;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class DeclaredFieldsOrderTest {

  private static class C {
    @Column(id = 1)
    private String c;
    @Column(id = 2)
    private String b;
    @Column(id = 3)
    private String a;

    private String d;
    private String e;
    private String f;
  }

  @Test
  public void testFieldSorting() {
    List<Field> fields = Arrays.asList(C.class.getDeclaredFields());
    for (List<Field> p : permutations(fields)) {
      List<Field> sorted = sort(p);
      assertEquals("c", sorted.get(0).getName());
      assertEquals("b", sorted.get(1).getName());
      assertEquals("a", sorted.get(2).getName());
      assertEquals("d", sorted.get(3).getName());
      assertEquals("e", sorted.get(4).getName());
      assertEquals("f", sorted.get(5).getName());
    }
  }
}
