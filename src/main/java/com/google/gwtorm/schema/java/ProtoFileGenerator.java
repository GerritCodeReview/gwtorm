// Copyright 2010 Google Inc.
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

import com.google.gwtorm.client.Column;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.RelationModel;

import org.objectweb.asm.Type;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

class ProtoFileGenerator {
  private static final Comparator<ColumnModel> COLUMN_COMPARATOR =
      new Comparator<ColumnModel>() {
        @Override
        public int compare(ColumnModel o1, ColumnModel o2) {
          return o1.getColumnID() - o2.getColumnID();
        }
      };

  private static final Comparator<RelationModel> RELATION_COMPARATOR =
      new Comparator<RelationModel>() {
        @Override
        public int compare(RelationModel o1, RelationModel o2) {
          return o1.getRelationId() - o2.getRelationId();
        }
      };

  private final Collection<RelationModel> rels;
  private final String schemaName;
  private final HashSet<String> seen;
  private final HashSet<String> collisions;

  ProtoFileGenerator(String schemaName, Collection<RelationModel> relations) {
    this.schemaName = schemaName;
    this.rels = relations;
    this.seen = new HashSet<String>();
    this.collisions = new HashSet<String>();
  }

  void print(PrintWriter out) {
    seen.clear();
    collisions.clear();

    for (RelationModel r : rels) {
      for (ColumnModel c : r.getColumns()) {
        if (c.isNested()) {
          String type = getShortClassName(c);
          if (seen.contains(type)) {
            collisions.add(type);
          } else {
            seen.add(type);
          }
        }
      }
    }

    seen.clear();
    for (RelationModel r : rels) {
      generateMessage(r, out);
    }

    out.print("message " + schemaName + " {\n");

    for (RelationModel r : sortRelations(rels)) {
      out.print("\toptional " + getName(r) + " "
          + r.getRelationName().toLowerCase() + " = " + r.getRelationId()
          + ";\n");
    }

    out.print("}\n");
  }

  private void generateMessage(RelationModel rel, PrintWriter out) {
    List<ColumnModel> cols = sortColumns(rel.getFields());
    for (ColumnModel c : cols) {
      generateMessage(c, out);
    }

    out.print("message " + getName(rel) + " {\n");
    for (ColumnModel c : cols) {
      out.append("\toptional " + getType(c) + " " + getName(c) + " = "
          + c.getColumnID() + ";\n");
    }
    out.print("}\n\n");
  }

  private void generateMessage(ColumnModel parent, PrintWriter out) {
    // Handle base cases
    if (!parent.isNested()) {
      return;
    } else if (seen.contains(parent.getNestedClassName())) {
      return;
    }

    List<ColumnModel> children = sortColumns(parent.getNestedColumns());
    for (ColumnModel child : children) {
      generateMessage(child, out);
    }

    out.print("message " + getType(parent) + " {\n");
    for (ColumnModel child : children) {
      out.append("\toptional " + getType(child) + " " + getName(child) + " = "
          + child.getColumnID() + ";\n");
    }
    out.print("}\n\n");

    seen.add(parent.getNestedClassName());
  }

  private String getType(ColumnModel cm) {
    if (cm.isNested()) {
      String type = getShortClassName(cm);
      if (collisions.contains(type)) {
        return cm.getNestedClassName().replace('.', '_').replace('$', '_');
      } else {
        return type;
      }
    } else {
      return toProtoType(cm.getPrimitiveType());
    }
  }

  private static String getName(ColumnModel cm) {
    if (cm.getColumnName().equals(Column.NONE)) {
      return cm.getFieldName();
    } else {
      return cm.getColumnName();
    }
  }

  private static String getShortClassName(ColumnModel cm) {
    String tmp = cm.getNestedClassName();
    return tmp.substring(tmp.lastIndexOf('.') + 1).replace('$', '_');
  }

  private static String getName(RelationModel r) {
    String typeName = r.getEntityTypeClassName();
    return typeName.substring(typeName.lastIndexOf('.') + 1);
  }

  private static List<ColumnModel> sortColumns(Collection<ColumnModel> cols) {
    ArrayList<ColumnModel> list = new ArrayList<ColumnModel>(cols);
    Collections.sort(list, COLUMN_COMPARATOR);
    return list;
  }

  private static List<RelationModel> sortRelations(
      Collection<RelationModel> rels) {
    ArrayList<RelationModel> list = new ArrayList<RelationModel>(rels);
    Collections.sort(list, RELATION_COMPARATOR);
    return list;
  }

  private static String toProtoType(Class<?> clazz) {
    switch (Type.getType(clazz).getSort()) {
      case Type.BOOLEAN:
        return "bool";
      case Type.CHAR:
        return "uint32";
      case Type.BYTE:
      case Type.SHORT:
      case Type.INT:
        return "sint32";
      case Type.FLOAT:
        return "float";
      case Type.DOUBLE:
        return "double";
      case Type.LONG:
        return "sint64";
      case Type.ARRAY:
      case Type.OBJECT: {
        if (clazz == byte[].class) {
          return "bytes";
        } else if (clazz == String.class) {
          return "string";
        } else if (clazz == java.sql.Timestamp.class) {
          return "fixed64";
        } else if (clazz.isEnum()) {
          return "enum"; // TODO handle enums
        } else {
          throw new RuntimeException("Type " + clazz
              + " not supported on protobuf!");
        }
      }

      default:
        throw new RuntimeException("Type " + clazz
            + " not supported on protobuf!");
    }
  }
}
