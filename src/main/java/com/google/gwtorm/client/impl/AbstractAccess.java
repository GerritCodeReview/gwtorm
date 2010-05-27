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

package com.google.gwtorm.client.impl;

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractAccess<E, K extends Key<?>>
    implements Access<E, K> {
  public ResultSet<E> get(final Iterable<K> keys) throws OrmException {
    final ArrayList<E> r = new ArrayList<E>();
    for (final K key : keys) {
      final E o = get(key);
      if (o != null) {
        r.add(o);
      }
    }
    return new ListResultSet<E>(r);
  }

  public Map<K, E> toMap(final Iterable<E> c) {
    try {
      final HashMap<K, E> r = new HashMap<K, E>();
      for (final E e : c) {
        r.put(primaryKey(e), e);
      }
      return r;
    } finally {
      if (c instanceof ResultSet) {
        ((ResultSet<?>) c).close();
      }
    }
  }

  public final void insert(final Iterable<E> instances) throws OrmException {
    doInsert(instances);
  }

  public final void update(final Iterable<E> instances) throws OrmException {
    doUpdate(instances);
  }

  public final void upsert(final Iterable<E> instances) throws OrmException {
    doUpsert(instances);
  }

  public final void delete(final Iterable<E> instances) throws OrmException {
    doDelete(instances);
  }

  protected abstract void doInsert(Iterable<E> instances)
      throws OrmException;

  protected abstract void doUpdate(Iterable<E> instances)
      throws OrmException;

  protected abstract void doUpsert(Iterable<E> instances)
      throws OrmException;

  protected abstract void doDelete(Iterable<E> instances)
      throws OrmException;

}
