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
import com.google.gwt.core.ext.typeinfo.JGenericType;
import com.google.gwt.core.ext.typeinfo.JMethod;
import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.Column;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.PrimaryKey;
import com.google.gwtorm.client.Query;
import com.google.gwtorm.client.Relation;
import com.google.gwtorm.schema.QueryModel;
import com.google.gwtorm.schema.RelationModel;

import java.util.ArrayList;
import java.util.List;

class GwtRelationModel extends RelationModel {
  private final JMethod method;
  private final JClassType accessType;
  private final JClassType entityType;

  GwtRelationModel(final JMethod m) throws OrmException {
    method = m;
    initName(method.getName(), m.getAnnotation(Relation.class));

    accessType = method.getReturnType().isInterface();
    if (accessType == null) {
      throw new OrmException("Method " + method.getName() + " in "
          + method.getEnclosingType().getQualifiedSourceName()
          + " must return an extension of " + Access.class);
    }

    if (accessType.getImplementedInterfaces().length != 1
        || !accessType.getImplementedInterfaces()[0].getQualifiedSourceName()
            .equals(Access.class.getName())) {
      throw new OrmException("Method " + method.getName() + " in "
          + method.getEnclosingType().getQualifiedSourceName()
          + " must return a direct extension of " + Access.class);
    }

    final JGenericType gt =
        accessType.getImplementedInterfaces()[0].isGenericType();
    if (gt == null) {
      throw new OrmException(accessType.getQualifiedSourceName()
          + " must specify entity type parameter for " + Access.class);
    }
    entityType = gt.getTypeParameters()[0];

    initColumns();
    initQueriesAndKeys();
  }

  private void initColumns() throws OrmException {
    final List<GwtColumnModel> col = new ArrayList<GwtColumnModel>();
    JClassType in = entityType;
    while (in != null) {
      for (final JField f : in.getFields()) {
        if (f.getAnnotation(Column.class) != null) {
          col.add(new GwtColumnModel(f));
        }
      }
      in = in.getSuperclass();
    }
    initColumns(col);
  }

  private void initQueriesAndKeys() throws OrmException {
    for (final JMethod m : accessType.getMethods()) {
      if (m.getAnnotation(PrimaryKey.class) != null) {
        if (!m.getReturnType().getQualifiedSourceName().equals(
            entityType.getQualifiedSourceName())) {
          throw new OrmException("PrimaryKey " + m.getName() + " must return "
              + entityType.getName());
        }
        initPrimaryKey(m.getName(), m.getAnnotation(PrimaryKey.class));

      } else if (m.getAnnotation(Query.class) != null) {
        addQuery(new QueryModel(this, m.getName(), m.getAnnotation(Query.class)));
      }
    }
  }

  @Override
  public String getAccessInterfaceName() {
    return accessType.getQualifiedSourceName();
  }

  @Override
  public String getEntityTypeClassName() {
    return entityType.getQualifiedSourceName();
  }
}
