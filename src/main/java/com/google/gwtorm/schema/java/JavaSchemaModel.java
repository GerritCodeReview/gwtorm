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

import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.SequenceModel;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Relation;
import com.google.gwtorm.server.Schema;
import com.google.gwtorm.server.Sequence;

import java.io.PrintWriter;
import java.lang.reflect.Method;


public class JavaSchemaModel extends SchemaModel {
  private final Class<?> schema;

  public JavaSchemaModel(final Class<?> schemaInterface) throws OrmException {
    schema = schemaInterface;

    if (!schema.isInterface()) {
      throw new OrmException("Schema " + schema.getName()
          + " must be an interface");
    }

    if (schema.getInterfaces().length != 1
        || schema.getInterfaces()[0] != Schema.class) {
      throw new OrmException("Schema " + schema.getName()
          + " must only extend " + Schema.class.getName());
    }

    for (final Method m : schema.getDeclaredMethods()) {
      if (m.getAnnotation(Relation.class) != null) {
        add(new JavaRelationModel(m));
        continue;
      }

      final Sequence seq = m.getAnnotation(Sequence.class);
      if (seq != null) {
        add(new SequenceModel(m.getName(), seq, m.getReturnType()));
        continue;
      }
    }
  }

  public RelationModel getRelation(String name) {
    for (RelationModel m : getRelations()) {
      if (m.getMethodName().equals(name)) {
        return m;
      }
    }
    throw new IllegalArgumentException("No relation named " + name);
  }

  public void generateProto(PrintWriter out) {
    ProtoFileGenerator pfg = new ProtoFileGenerator(schema.getSimpleName(), getRelations());
    pfg.print(out);
  }

  @Override
  public String getSchemaClassName() {
    return schema.getName();
  }
}
