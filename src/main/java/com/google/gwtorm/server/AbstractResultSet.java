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

package com.google.gwtorm.server;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simple implementation of a {@link ResultSet}.
 *
 * @param <T> type of the object to be returned from the result set.
 */
public abstract class AbstractResultSet<T> implements ResultSet<T> {
  @Override
  public final Iterator<T> iterator() {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return AbstractResultSet.this.hasNext();
      }

      @Override
      public T next() {
        return AbstractResultSet.this.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public List<T> toList() {
    List<T> r = new ArrayList<T>();
    for (T obj : this) {
      r.add(obj);
    }
    return r;
  }

  /** @return true if another result remains, false otherwise. */
  protected abstract boolean hasNext();

  /** @return next result. */
  protected abstract T next();
}
