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

package com.google.gwtorm.data;

import com.google.gwtorm.server.Access;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.PrimaryKey;
import com.google.gwtorm.server.Query;
import com.google.gwtorm.server.ResultSet;

public interface PersonAccess extends Access<TestPerson, TestPerson.Key> {
  @PrimaryKey("name")
  TestPerson get(TestPerson.Key key) throws OrmException;

  @Query
  ResultSet<TestPerson> all() throws OrmException;

  @Query("WHERE age > ? ORDER BY age")
  ResultSet<TestPerson> olderThan(int age) throws OrmException;

  @Query("WHERE age > ? ORDER BY name DESC")
  ResultSet<TestPerson> olderThanDescByName(int age)
      throws OrmException;

  @Query("WHERE name = 'bob' LIMIT ?")
  ResultSet<TestPerson> firstNBob(int n) throws OrmException;

  @Query("WHERE registered = false ORDER BY name")
  ResultSet<TestPerson> notRegistered() throws OrmException;

  @Query("ORDER BY age LIMIT 1")
  ResultSet<TestPerson> youngest() throws OrmException;

  @Query("ORDER BY age LIMIT ?")
  ResultSet<TestPerson> youngestN(int n) throws OrmException;
}
