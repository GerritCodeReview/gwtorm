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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.gwtorm.client.Column;
import com.google.gwtorm.data.Address;
import com.google.gwtorm.data.Person;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;

public class ProtobufEncoderTest {
  private static final byte[] testingBin =
      new byte[] {
        //
        // name
        0x0a,
        0x09,
        // name.name
        0x0a,
        0x07,
        0x74,
        0x65,
        0x73,
        0x74,
        0x69,
        0x6e,
        0x67, //
        // age
        0x10,
        (byte) 75, //
        // registered (true)
        0x18,
        0x01 //
        //
      };

  @SuppressWarnings("cast")
  @Test
  public void testPerson() throws UnsupportedEncodingException {
    final ProtobufCodec<Person> e = CodecFactory.encoder(Person.class);

    Person p = e.decode(testingBin);
    assertNotNull(p);
    assertTrue(p instanceof Person);
    assertEquals("testing", p.name());
    assertEquals(75, p.age());
    assertTrue(p.isRegistered());

    final byte[] out = e.encodeToByteArray(p);
    assertEquals(asString(testingBin), asString(out));
    assertEquals(testingBin.length, e.sizeof(p));
  }

  @Test
  public void testAddress() {
    final ProtobufCodec<Address> e = CodecFactory.encoder(Address.class);
    Address a = e.decode(new byte[0]);
    assertNotNull(a);
    assertNull(a.location());

    Person.Key k = new Person.Key("bob");
    Address b = new Address(new Address.Key(k, "ny"), "ny");

    byte[] act = e.encodeToByteArray(b);

    Address c = e.decode(act);
    assertEquals(c.location(), b.location());
    assertEquals(c.city(), b.city());
    assertEquals(c.key(), b.key());
  }

  @Test
  public void testDecodeEmptiesByteBuffer() {
    ProtobufCodec<Person> e = CodecFactory.encoder(Person.class);
    ByteBuffer buf = ByteBuffer.wrap(testingBin);
    @SuppressWarnings("unused")
    Person p = e.decode(buf);
    assertEquals(0, buf.remaining());
    assertEquals(testingBin.length, buf.position());
  }

  @Test
  public void testEncodeFillsByteBuffer() throws UnsupportedEncodingException {
    ProtobufCodec<Person> e = CodecFactory.encoder(Person.class);

    Person p = new Person(new Person.Key("testing"), 75);
    p.register();

    int sz = e.sizeof(p);
    assertEquals(testingBin.length, sz);

    ByteBuffer buf = ByteBuffer.allocate(sz);
    e.encode(p, buf);
    assertEquals(0, buf.remaining());
    assertEquals(sz, buf.position());

    buf.flip();
    byte[] act = new byte[sz];
    buf.get(act);

    assertEquals(asString(testingBin), asString(act));
  }

  @Test
  public void testEncodeNonArrayByteBuffer() throws UnsupportedEncodingException {
    ProtobufCodec<Person> e = CodecFactory.encoder(Person.class);

    Person p = new Person(new Person.Key("testing"), 75);
    p.register();

    int sz = e.sizeof(p);
    assertEquals(testingBin.length, sz);

    ByteBuffer buf = ByteBuffer.allocateDirect(sz);
    assertFalse("direct ByteBuffer has no array", buf.hasArray());

    e.encode(p, buf);
    assertEquals(0, buf.remaining());
    assertEquals(sz, buf.position());

    buf.flip();
    byte[] act = new byte[sz];
    buf.get(act);

    assertEquals(asString(testingBin), asString(act));
  }

  @Test
  public void testStringList() throws UnsupportedEncodingException {
    ProtobufCodec<StringList> e = CodecFactory.encoder(StringList.class);

    StringList list = new StringList();
    list.list = new ArrayList<>();
    list.list.add("moe");
    list.list.add("larry");

    byte[] act = e.encodeToByteArray(list);
    StringList other = e.decode(act);
    assertNotNull(other.list);
    assertEquals(list.list, other.list);
    assertEquals(
        asString(
            new byte[] { //
              //
              0x12,
              0x03,
              'm',
              'o',
              'e', //
              0x12,
              0x05,
              'l',
              'a',
              'r',
              'r',
              'y' //
            }),
        asString(act));
  }

  @Test
  public void testStringSet() throws UnsupportedEncodingException {
    ProtobufCodec<StringSet> e = CodecFactory.encoder(StringSet.class);

    StringSet list = new StringSet();
    list.list = new TreeSet<>();
    list.list.add("larry");
    list.list.add("moe");

    byte[] act = e.encodeToByteArray(list);
    StringSet other = e.decode(act);
    assertNotNull(other.list);
    assertEquals(list.list, other.list);
    assertEquals(
        asString(
            new byte[] { //
              //
              0x0a,
              0x05,
              'l',
              'a',
              'r',
              'r',
              'y', //
              0x0a,
              0x03,
              'm',
              'o',
              'e' //
            }),
        asString(act));
  }

  @Test
  public void testPersonList() {
    ProtobufCodec<PersonList> e = CodecFactory.encoder(PersonList.class);

    PersonList list = new PersonList();
    list.people = new ArrayList<>();
    list.people.add(new Person(new Person.Key("larry"), 1 << 16));
    list.people.add(new Person(new Person.Key("curly"), 1));
    list.people.add(new Person(new Person.Key("moe"), -1));

    PersonList other = e.decode(e.encodeToByteArray(list));
    assertNotNull(other.people);
    assertEquals(list.people, other.people);
  }

  @Test
  public void testCustomEncoderList() {
    ProtobufCodec<ItemList> e = CodecFactory.encoder(ItemList.class);

    ItemList list = new ItemList();
    list.list = new ArrayList<>();
    list.list.add(new Item());
    list.list.add(new Item());

    ItemList other = e.decode(e.encodeToByteArray(list));
    assertNotNull(other.list);
    assertEquals(2, other.list.size());
  }

  @Test
  public void testEnumEncoder() throws UnsupportedEncodingException {
    assertEquals(1, ThingWithEnum.Type.B.ordinal());
    assertSame(ThingWithEnum.Type.B, ThingWithEnum.Type.values()[1]);

    ProtobufCodec<ThingWithEnum> e = CodecFactory.encoder(ThingWithEnum.class);

    ThingWithEnum thing = new ThingWithEnum();
    thing.type = ThingWithEnum.Type.B;

    ThingWithEnum other = e.decode(e.encodeToByteArray(thing));
    assertNotNull(other.type);
    assertSame(thing.type, other.type);

    byte[] act = e.encodeToByteArray(thing);
    byte[] exp = {0x08, 0x01};
    assertEquals(asString(exp), asString(act));
  }

  @Test
  public void testEncodeToStream() throws IOException {
    ProtobufCodec<ThingWithEnum> e = CodecFactory.encoder(ThingWithEnum.class);

    ThingWithEnum thing = new ThingWithEnum();
    thing.type = ThingWithEnum.Type.B;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    e.encodeWithSize(thing, out);
    byte[] exp = {0x02, 0x08, 0x01};
    assertEquals(asString(exp), asString(out.toByteArray()));

    byte[] exp2 = {0x02, 0x08, 0x01, '\n'};
    ByteArrayInputStream in = new ByteArrayInputStream(exp2);
    ThingWithEnum other = e.decodeWithSize(in);
    assertTrue('\n' == in.read());
    assertTrue(-1 == in.read());
    assertNotNull(other.type);
    assertSame(thing.type, other.type);
  }

  private static String asString(byte[] bin) throws UnsupportedEncodingException {
    return new String(bin, "ISO-8859-1");
  }

  static class PersonList {
    @Column(id = 5)
    public List<Person> people;
  }

  static class StringList {
    @Column(id = 2)
    List<String> list;
  }

  static class StringSet {
    @Column(id = 1)
    SortedSet<String> list;
  }

  static class Item {}

  static class ItemCodec extends ProtobufCodec<Item> {
    @Override
    public void encode(Item obj, CodedOutputStream out) throws IOException {
      out.writeBoolNoTag(true);
    }

    @Override
    public void mergeFrom(CodedInputStream in, Item obj) throws IOException {
      in.readBool();
    }

    @Override
    public Item newInstance() {
      return new Item();
    }

    @Override
    public int sizeof(Item obj) {
      return 1;
    }
  }

  static class ItemList {
    @Column(id = 2)
    @CustomCodec(ItemCodec.class)
    List<Item> list;
  }

  static class ThingWithEnum {
    static enum Type {
      A,
      B;
    }

    @Column(id = 1)
    Type type;
  }
}
