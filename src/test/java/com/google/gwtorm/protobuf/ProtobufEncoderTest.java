// Copyright 2009 Google Inc.
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

package com.google.gwtorm.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.gwtorm.data.Address;
import com.google.gwtorm.data.Person;

import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class ProtobufEncoderTest {
  @SuppressWarnings("cast")
  @Test
  public void testPerson() throws UnsupportedEncodingException {
    final ProtobufCodec<Person> e = CodecFactory.encoder(Person.class);
    final byte[] bin = new byte[] {
    //
        // name
        0x0a, 0x09,
        // name.name
        0x0a, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, //
        // age
        0x10, (byte) 0x96, 0x01, //
        // registered (true)
        0x18, 0x01 //
        //
        };
    Person p = e.decode(bin);
    assertNotNull(p);
    assertTrue(p instanceof Person);
    assertEquals("testing", p.name());
    assertEquals(75, p.age());
    assertTrue(p.isRegistered());

    final byte[] out = e.encode(p).toByteArray();
    assertEquals(new String(bin, "ISO-8859-1"), new String(out, "ISO-8859-1"));
    assertEquals(bin.length, e.sizeof(p));
  }

  @Test
  public void testAddress() {
    final ProtobufCodec<Address> e =
        CodecFactory.encoder(Address.class);
    Address a = e.decode(new byte[0]);
    assertNotNull(a);
    assertNull(a.location());

    Person.Key k = new Person.Key("bob");
    Person p = new Person(k, 42);
    Address b = new Address(new Address.Key(k, "ny"), "ny");

    byte[] act = e.encode(b).toByteArray();

    Address c = e.decode(act);
    assertEquals(c.location(), b.location());
    assertEquals(c.city(), b.city());
    assertEquals(c.key(), b.key());
  }
}
