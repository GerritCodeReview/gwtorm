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

package com.google.gwtorm.schema.java;

import com.google.common.collect.Ordering;
import com.google.gwtorm.client.Column;
import com.google.gwtorm.client.RowVersion;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.Util;
import com.google.gwtorm.server.OrmException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaColumnModel extends ColumnModel {
  public static List<Field> getDeclaredFields(Class<?> in) {
    return sort(Arrays.asList(in.getDeclaredFields()));
  }

  static List<Field> sort(List<Field> fields) {
    return new Ordering<Field>() {
      @Override
      public int compare(Field f1, Field f2) {
        Column a1 = f1.getAnnotation(Column.class);
        Column a2 = f2.getAnnotation(Column.class);
        if (a1 == null && a2 == null) {
          return f1.getName().compareTo(f2.getName());
        }
        if (a1 == null) {
          return 1;
        }
        if (a2 == null) {
          return -1;
        }
        return a1.id() - a2.id();
      }
    }.sortedCopy(fields);
  }

  private final Field field;
  private final String fieldName;
  private final Class<?> primitiveType;
  private final Type genericType;

  public JavaColumnModel(final Field f) throws OrmException {
    field = f;
    fieldName = field.getName();
    primitiveType = field.getType();
    genericType = field.getGenericType();

    initName(fieldName, field.getAnnotation(Column.class));

    if (Modifier.isPrivate(field.getModifiers())) {
      throw new OrmException(
          "Field "
              + field.getName()
              + " of "
              + field.getDeclaringClass().getName()
              + " must not be private");
    }
    if (Modifier.isFinal(field.getModifiers())) {
      throw new OrmException(
          "Field "
              + field.getName()
              + " of "
              + field.getDeclaringClass().getName()
              + " must not be final");
    }

    rowVersion = field.getAnnotation(RowVersion.class) != null;
    if (rowVersion && field.getType() != Integer.TYPE) {
      throw new OrmException(
          "Field "
              + field.getName()
              + " of "
              + field.getDeclaringClass().getName()
              + " must have type 'int'");
    }

    initNested();
  }

  public JavaColumnModel(
      Field f, final String fieldPath, final int columnId, final Class<?> columnType)
      throws OrmException {
    this.field = f;
    this.fieldName = fieldPath;
    this.columnName = fieldPath;
    this.columnId = columnId;
    this.primitiveType = columnType;
    this.genericType = null;
    initNested();
  }

  private void initNested() throws OrmException {
    if (isNested()) {
      final List<JavaColumnModel> col = new ArrayList<>();
      Class<?> in = primitiveType;
      while (in != null) {
        for (final Field f : getDeclaredFields(in)) {
          if (f.getAnnotation(Column.class) != null) {
            col.add(new JavaColumnModel(f));
          }
        }
        in = in.getSuperclass();
      }
      initNestedColumns(col);
    }
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public Class<?> getPrimitiveType() {
    return isPrimitive() ? primitiveType : null;
  }

  @Override
  public Type[] getArgumentTypes() {
    if (genericType instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) genericType;
      return pt.getActualTypeArguments();
    }
    return new Type[0];
  }

  @Override
  public String getNestedClassName() {
    return isPrimitive() ? null : primitiveType.getName();
  }

  @Override
  public boolean isCollection() {
    return java.util.Collection.class.isAssignableFrom(primitiveType);
  }

  public Class<?> getNestedClass() {
    return primitiveType;
  }

  public Field getField() {
    return field;
  }

  private boolean isPrimitive() {
    return Util.isSqlPrimitive(primitiveType);
  }
}
