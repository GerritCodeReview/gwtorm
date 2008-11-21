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
import com.google.gwtorm.client.PrimaryKey;
import com.google.gwtorm.client.Relation;
import com.google.gwtorm.schema.sql.SqlDialect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;

public abstract class RelationModel {
  protected String methodName;
  protected String relationName;
  protected Relation relation;
  protected final LinkedHashMap<String, ColumnModel> fieldsByFieldName;
  protected final LinkedHashMap<String, ColumnModel> columnsByColumnName;
  protected KeyModel primaryKey;
  protected Collection<QueryModel> queries;

  protected RelationModel() {
    fieldsByFieldName = new LinkedHashMap<String, ColumnModel>();
    columnsByColumnName = new LinkedHashMap<String, ColumnModel>();
    queries = new ArrayList<QueryModel>();
  }

  protected void initName(final String method, final Relation rel)
      throws OrmException {
    if (rel == null) {
      throw new OrmException("Method " + method + " is missing "
          + Relation.class.getName() + " annotation");
    }
    relation = rel;
    methodName = method;
    relationName = Util.any(relation.name(), methodName);
  }

  protected void initColumns(final Collection<? extends ColumnModel> allFields)
      throws OrmException {
    for (final ColumnModel field : allFields) {
      if (fieldsByFieldName.put(field.getFieldName(), field) != null) {
        throw new OrmException("Duplicate fields " + field.getFieldName());
      }

      if (field.isNested()) {
        for (final ColumnModel newCol : field.getAllLeafColumns()) {
          registerColumn(newCol);
        }
      } else {
        registerColumn(field);
      }
    }
  }

  private void registerColumn(final ColumnModel nc) throws OrmException {
    final ColumnModel oc = columnsByColumnName.put(nc.getColumnName(), nc);
    if (oc != null) {
      throw new OrmException("Duplicate columns " + nc.getColumnName() + " in "
          + getMethodName() + ":\n" + "prior " + oc.getPathToFieldName()
          + "\n next  " + nc.getPathToFieldName());
    }
  }

  protected void initPrimaryKey(final String name, final PrimaryKey annotation)
      throws OrmException {
    if (primaryKey != null) {
      throw new OrmException("Duplicate primary key definitions");
    }

    final ColumnModel field = getField(annotation.value());
    if (field == null) {
      throw new OrmException("Field " + annotation.value() + " not in "
          + getEntityTypeClassName());
    }

    primaryKey = new KeyModel(name, field);
    for (final ColumnModel c : primaryKey.getAllLeafColumns()) {
      c.inPrimaryKey = true;
    }
  }

  protected void addQuery(final QueryModel q) {
    queries.add(q);
  }


  public String getMethodName() {
    return methodName;
  }

  public String getRelationName() {
    return relationName;
  }

  public Collection<ColumnModel> getDependentFields() {
    final ArrayList<ColumnModel> r = new ArrayList<ColumnModel>();
    for (final ColumnModel c : fieldsByFieldName.values()) {
      if (primaryKey == null || primaryKey.getField() != c) {
        r.add(c);
      }
    }
    return r;
  }

  public Collection<ColumnModel> getDependentColumns() {
    final ArrayList<ColumnModel> r = new ArrayList<ColumnModel>();
    for (final ColumnModel c : columnsByColumnName.values()) {
      if (!c.inPrimaryKey) {
        r.add(c);
      }
    }
    return r;
  }

  public KeyModel getPrimaryKey() {
    return primaryKey;
  }

  public Collection<ColumnModel> getPrimaryKeyColumns() {
    if (getPrimaryKey() != null) {
      return getPrimaryKey().getAllLeafColumns();
    }
    return Collections.<ColumnModel> emptyList();
  }

  public Collection<QueryModel> getQueries() {
    return queries;
  }

  public Collection<ColumnModel> getColumns() {
    final ArrayList<ColumnModel> r = new ArrayList<ColumnModel>();
    r.addAll(getDependentColumns());
    r.addAll(getPrimaryKeyColumns());
    return r;
  }

  public Collection<ColumnModel> getFields() {
    return fieldsByFieldName.values();
  }

  public ColumnModel getField(final String name) {
    return fieldsByFieldName.get(name);
  }

  public String getCreateTableSql(final SqlDialect dialect) {
    final StringBuilder r = new StringBuilder();
    r.append("CREATE TABLE ");
    r.append(relationName);
    r.append(" (\n");

    for (final Iterator<ColumnModel> i = getColumns().iterator(); i.hasNext();) {
      final ColumnModel col = i.next();
      r.append(col.getColumnName());
      r.append(" ");
      r.append(dialect.getSqlTypeInfo(col).getSqlType(col));
      if (i.hasNext()) {
        r.append(",");
      } else if (!getPrimaryKeyColumns().isEmpty()) {
        r.append(",");
      }
      r.append("\n");
    }

    if (!getPrimaryKeyColumns().isEmpty()) {
      r.append("PRIMARY KEY(");
      for (final Iterator<ColumnModel> i = getPrimaryKeyColumns().iterator(); i
          .hasNext();) {
        final ColumnModel col = i.next();
        r.append(col.getColumnName());
        if (i.hasNext()) {
          r.append(",");
        }
      }
      r.append(")\n");
    }

    r.append(")");
    return r.toString();
  }

  public String getSelectSql(final SqlDialect dialect, final String tableAlias) {
    final StringBuilder r = new StringBuilder();
    r.append("SELECT ");
    for (final Iterator<ColumnModel> i = getColumns().iterator(); i.hasNext();) {
      final ColumnModel col = i.next();
      r.append(tableAlias);
      r.append('.');
      r.append(col.getColumnName());
      if (i.hasNext()) {
        r.append(",");
      }
    }
    r.append(" FROM ");
    r.append(relationName);
    r.append(' ');
    r.append(tableAlias);
    return r.toString();
  }

  public String getInsertOneSql(final SqlDialect dialect) {
    final StringBuilder r = new StringBuilder();
    r.append("INSERT INTO ");
    r.append(relationName);
    r.append("(");
    for (final Iterator<ColumnModel> i = getColumns().iterator(); i.hasNext();) {
      final ColumnModel col = i.next();
      r.append(col.getColumnName());
      if (i.hasNext()) {
        r.append(",");
      }
    }
    r.append(")VALUES(");
    int nth = 1;
    for (final Iterator<ColumnModel> i = getColumns().iterator(); i.hasNext();) {
      i.next();
      r.append(dialect.getParameterPlaceHolder(nth++));
      if (i.hasNext()) {
        r.append(",");
      }
    }
    r.append(")");
    return r.toString();
  }

  public String getUpdateOneSql(final SqlDialect dialect) {
    final StringBuilder r = new StringBuilder();
    r.append("UPDATE ");
    r.append(relationName);
    r.append(" SET ");
    int nth = 1;
    for (final Iterator<ColumnModel> i = getDependentColumns().iterator(); i
        .hasNext();) {
      final ColumnModel col = i.next();
      r.append(col.getColumnName());
      r.append("=");
      r.append(dialect.getParameterPlaceHolder(nth++));
      if (i.hasNext()) {
        r.append(",");
      }
    }
    r.append(" WHERE ");
    for (final Iterator<ColumnModel> i = getPrimaryKeyColumns().iterator(); i
        .hasNext();) {
      final ColumnModel col = i.next();
      r.append(col.getColumnName());
      r.append("=");
      r.append(dialect.getParameterPlaceHolder(nth++));
      if (i.hasNext()) {
        r.append(" AND ");
      }
    }
    return r.toString();
  }

  public String getDeleteOneSql(final SqlDialect dialect) {
    final StringBuilder r = new StringBuilder();
    r.append("DELETE FROM ");
    r.append(relationName);
    int nth = 1;
    r.append(" WHERE ");
    for (final Iterator<ColumnModel> i = getPrimaryKeyColumns().iterator(); i
        .hasNext();) {
      final ColumnModel col = i.next();
      r.append(col.getColumnName());
      r.append("=");
      r.append(dialect.getParameterPlaceHolder(nth++));
      if (i.hasNext()) {
        r.append(" AND ");
      }
    }
    return r.toString();
  }

  public abstract String getAccessInterfaceName();

  public abstract String getEntityTypeClassName();

  @Override
  public String toString() {
    final StringBuilder r = new StringBuilder();
    r.append("Relation[\n");
    r.append("  method: " + getMethodName() + "\n");
    r.append("  table:  " + getRelationName() + "\n");
    r.append("  access: " + getAccessInterfaceName() + "\n");
    r.append("  entity: " + getEntityTypeClassName() + "\n");
    r.append("]");
    return r.toString();
  }
}
