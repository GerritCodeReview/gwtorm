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

package com.google.gwtorm.schema.gwt;

import com.google.gwt.core.ext.typeinfo.JClassType;
import com.google.gwt.core.ext.typeinfo.JField;
import com.google.gwt.core.ext.typeinfo.JPrimitiveType;
import com.google.gwt.core.ext.typeinfo.JType;
import com.google.gwtorm.client.Column;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.RowVersion;
import com.google.gwtorm.schema.ColumnModel;

import java.util.ArrayList;
import java.util.List;


class GwtColumnModel extends ColumnModel {
  static Class<?> toClass(final JType type) {
    if (type.isPrimitive() == JPrimitiveType.BOOLEAN) {
      return Boolean.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.BYTE) {
      return Byte.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.SHORT) {
      return Short.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.CHAR) {
      return Character.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.INT) {
      return Integer.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.LONG) {
      return Long.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.FLOAT) {
      return Float.TYPE;
    }

    if (type.isPrimitive() == JPrimitiveType.DOUBLE) {
      return Double.TYPE;
    }

    if (type.isArray() != null && type.isArray().getRank() == 1
        && type.isArray().getComponentType() == JPrimitiveType.BYTE) {
      return byte[].class;
    }

    if (type.isClass() != null
        && type.getQualifiedSourceName().startsWith("java.")) {
      try {
        return Class.forName(type.getQualifiedSourceName());
      } catch (ClassNotFoundException err) {
        // Never happen, but fall through anyway
      }
    }

    return null;
  }

  private final JField field;
  private final Class<?> primitiveType;

  GwtColumnModel(final JField columnField) throws OrmException {
    field = columnField;
    initName(field.getName(), field.getAnnotation(Column.class));

    if (field.isPrivate()) {
      throw new OrmException("Field " + getFieldName() + " of "
          + field.getEnclosingType().getQualifiedSourceName()
          + " must not be private");
    }
    if (field.isFinal()) {
      throw new OrmException("Field " + getFieldName() + " of "
          + field.getEnclosingType().getQualifiedSourceName()
          + " must not be final");
    }

    primitiveType = toClass(field.getType());
    rowVersion = field.getAnnotation(RowVersion.class) != null;
    if (rowVersion && primitiveType != Integer.TYPE) {
      throw new OrmException("Field " + field.getName() + " of "
          + field.getEnclosingType().getQualifiedSourceName()
          + " must have type 'int'");
    }

    if (isNested()) {
      final List<GwtColumnModel> col = new ArrayList<GwtColumnModel>();
      JClassType in = field.getType().isClass();
      while (in != null) {
        for (final JField f : in.getFields()) {
          if (f.getAnnotation(Column.class) != null) {
            col.add(new GwtColumnModel(f));
          }
        }
        in = in.getSuperclass();
      }
      initNestedColumns(col);
    }
  }

  @Override
  public String getFieldName() {
    return field.getName();
  }

  @Override
  public Class<?> getPrimitiveType() {
    return primitiveType;
  }

  @Override
  public String getNestedClassName() {
    if (primitiveType == null) {
      return field.getType().getQualifiedSourceName();
    }
    return null;
  }
}
