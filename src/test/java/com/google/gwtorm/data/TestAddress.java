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

public class TestAddress {
  public static class Key extends StringKey<TestPerson.Key> {
    @Column
    protected TestPerson.Key owner;

    @Column
    protected String name;

    protected Key() {
      owner = new TestPerson.Key();
    }

    public Key(final TestPerson.Key owner, final String name) {
      this.owner = owner;
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }

    @Override
    public TestPerson.Key getParentKey() {
      return owner;
    }
  }

  @Column
  protected Key city;

  @Column(length = Integer.MAX_VALUE)
  protected String location;

  @Column(notNull = false)
  protected byte[] photo;

  protected TestAddress() {
  }

  public TestAddress(final TestAddress.Key city, final String where) {
    this.city = city;
    this.location = where;
  }

  public String city() {
    return city.name;
  }

  public String location() {
    return location;
  }
}
