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


public class TestPerson {
  public static class Key extends StringKey<com.google.gwtorm.client.Key<?>> {
    @Column(length = 20)
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

  @Column
  protected Key name;

  @Column
  protected int age;

  @Column
  protected boolean registered;

  protected TestPerson() {
  }

  public TestPerson(final Key key, final int age) {
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
}
