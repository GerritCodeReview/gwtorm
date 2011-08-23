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

import com.google.gwtorm.client.Access;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.PrimaryKey;
import com.google.gwtorm.client.Query;
import com.google.gwtorm.client.ResultSet;

public interface PersonAccess extends Access<Person, Person.Key> {
  @PrimaryKey("name")
  Person get(Person.Key key) throws OrmException;

  @Query
  ResultSet<Person> all() throws OrmException;

  @Query("WHERE age > ? ORDER BY age")
  ResultSet<Person> olderThan(int age) throws OrmException;

  @Query("WHERE name != ? AND age > ? ORDER BY name DESC")
  ResultSet<Person> notPerson(Person.Key key, int age)
      throws OrmException;

  @Query("WHERE name = 'bob' LIMIT ?")
  ResultSet<Person> firstNBob(int n) throws OrmException;

  @Query("WHERE registered = false ORDER BY name")
  ResultSet<Person> notRegistered() throws OrmException;

  @Query("ORDER BY age LIMIT 1")
  ResultSet<Person> youngest() throws OrmException;

  @Query("ORDER BY age LIMIT ?")
  ResultSet<Person> youngestN(int n) throws OrmException;
}
