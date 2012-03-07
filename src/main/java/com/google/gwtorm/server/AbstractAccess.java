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

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.gwtorm.client.Key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractAccess<E, K extends Key<?>>
    implements Access<E, K> {
  private static final int MAX_TRIES = 10;

  @Override
  public void beginTransaction(K key) throws OrmException {
    // Do nothing by default.
  }

  public CheckedFuture<E, OrmException> getAsync(K key) {
    try {
      return Futures.immediateCheckedFuture(get(key));
    } catch (OrmException e) {
      return Futures.immediateFailedCheckedFuture(e);
    }
  }

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

  @Override
  public E atomicUpdate(final K key, final AtomicUpdate<E> update)
      throws OrmException {
    for (int attempts = 1;; attempts++) {
      try {
        final E obj = get(key);
        if (obj == null) {
          return null;
        }
        final E res = update.update(obj);
        update(Collections.singleton(obj));
        return res;
      } catch (OrmConcurrencyException err) {
        if (attempts < MAX_TRIES) {
          continue;
        }
        throw err;
      }
    }
  }

  @Override
  public void deleteKeys(Iterable<K> keys) throws OrmException {
    delete(get(keys));
  }
}
