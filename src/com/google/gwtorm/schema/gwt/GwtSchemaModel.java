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
import com.google.gwt.core.ext.typeinfo.JMethod;
import com.google.gwt.core.ext.typeinfo.JPrimitiveType;
import com.google.gwt.core.ext.typeinfo.JType;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Relation;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.client.Sequence;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.SequenceModel;

public class GwtSchemaModel extends SchemaModel {
  private final JClassType schema;

  public GwtSchemaModel(final JClassType schemaInterface) throws OrmException {
    schema = schemaInterface;

    if (schema.isInterface() == null) {
      throw new OrmException("Schema " + schema.getName()
          + " must be an interface");
    }

    if (schema.getImplementedInterfaces().length != 1
        || !schema.getImplementedInterfaces()[0].getQualifiedSourceName()
            .equals(Schema.class.getName())) {
      throw new OrmException("Schema " + schema.getName()
          + " must only extend " + Schema.class.getName());
    }

    for (final JMethod m : schema.getMethods()) {
      if (m.getAnnotation(Relation.class) != null) {
        add(new GwtRelationModel(m));
        continue;
      }

      final Sequence seq = m.getAnnotation(Sequence.class);
      if (seq != null) {
        final JType returnType = m.getReturnType();
        final Class<?> r;
        if (returnType == JPrimitiveType.INT) {
          r = Integer.TYPE;
        } else if (returnType == JPrimitiveType.LONG) {
          r = Long.TYPE;
        } else {
          r = Object.class;
        }
        add(new SequenceModel(m.getName(), seq, r));
        continue;
      }
    }
  }

  @Override
  public String getSchemaClassName() {
    return schema.getQualifiedSourceName();
  }
}
