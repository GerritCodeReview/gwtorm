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

import com.google.gwtorm.client.Column;
import com.google.gwtorm.client.StringKey;


public class Person {
  public static class Key extends StringKey<com.google.gwtorm.client.Key<?>> {
    @Column(id = 1, length = 20)
    protected String name;

    protected Key() {
    }

    public Key(final String name) {
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }

    @Override
    protected void set(String newValue) {
      name = newValue;
    }
  }

  @Column(id = 1)
  protected Key name;

  @Column(id = 2)
  protected int age;

  @Column(id = 3)
  protected boolean registered;

  protected Person() {
  }

  public Person(final Key key, final int age) {
    this.name = key;
    this.age = age;
  }

  public String name() {
    return name.get();
  }

  public int age() {
    return age;
  }

  public boolean isRegistered() {
    return registered;
  }

  public void growOlder() {
    age++;
  }

  public void register() {
    registered = true;
  }

  public void unregister() {
    registered = false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Person) {
      Person p = (Person) other;
      return name.equals(p.name) && age == p.age && registered == p.registered;
    }
    return false;
  }
}
