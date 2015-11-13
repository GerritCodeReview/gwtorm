// Copyright 2015 Google Inc.
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

package com.google.gwtorm.jdbc;

import com.google.gwtorm.server.OrmException;

import java.util.concurrent.TimeUnit;

/** Connections monitoring for {@link Database}. */
public interface DatabaseMetrics {
  public static enum Operation {
    SELECT, INSERT, UPDATE, DELETE;
  }

  public static final DatabaseMetrics DISABLED = new DatabaseMetrics() {
    @Override
    public void recordNextLong(String pool, long time, TimeUnit unit) {
    }

    @Override
    public void recordAccess(String relation, Operation op, String query,
        long time, TimeUnit unit) {
    }

    @Override
    public void recordOpenFailure(OrmException err) {
    }
  };

  public void recordNextLong(String pool, long time, TimeUnit unit);

  public void recordAccess(
      String relation, Operation op, String query,
      long time, TimeUnit unit);

  public void recordOpenFailure(OrmException err);
}
