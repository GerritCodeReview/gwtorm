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

public class Address {
  @SuppressWarnings("serial")
  public static class Key extends StringKey<Person.Key> {
    @Column(id = 1)
    protected Person.Key owner;

    @Column(id = 2)
    protected String name;

    protected Key() {
      owner = new Person.Key();
    }

    public Key(final Person.Key owner, final String name) {
      this.owner = owner;
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }

    @Override
    public Person.Key getParentKey() {
      return owner;
    }

    @Override
    protected void set(String newValue) {
      name = newValue;
    }
  }

  @Column(id = 1)
  protected Key city;

  @Column(id = 2, length = Integer.MAX_VALUE)
  protected String location;

  @Column(id = 3, notNull = false)
  protected byte[] photo;

  protected Address() {}

  public Address(final Address.Key city, final String where) {
    this.city = city;
    this.location = where;
  }

  public String city() {
    return city.name;
  }

  public String location() {
    return location;
  }

  public Address.Key key() {
    return city;
  }
}
