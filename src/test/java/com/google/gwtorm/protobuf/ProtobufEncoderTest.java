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

import com.google.gwtorm.data.TestAddress;
import com.google.gwtorm.data.TestPerson;

import junit.framework.TestCase;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class ProtobufEncoderTest extends TestCase {
  private static final byte[] testingBin = new byte[] {
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

  @SuppressWarnings("cast")
  public void testPerson() throws UnsupportedEncodingException {
    final ProtobufCodec<TestPerson> e = CodecFactory.encoder(TestPerson.class);

    TestPerson p = e.decode(testingBin);
    assertNotNull(p);
    assertTrue(p instanceof TestPerson);
    assertEquals("testing", p.name());
    assertEquals(75, p.age());
    assertTrue(p.isRegistered());

    final byte[] out = e.encodeToByteArray(p);
    assertEquals(asString(testingBin), asString(out));
    assertEquals(testingBin.length, e.sizeof(p));
  }

  public void testAddress() {
    final ProtobufCodec<TestAddress> e =
        CodecFactory.encoder(TestAddress.class);
    TestAddress a = e.decode(new byte[0]);
    assertNotNull(a);
    assertNull(a.location());

    TestPerson.Key k = new TestPerson.Key("bob");
    TestPerson p = new TestPerson(k, 42);
    TestAddress b = new TestAddress(new TestAddress.Key(k, "ny"), "ny");

    byte[] act = e.encodeToByteArray(b);

    TestAddress c = e.decode(act);
    assertEquals(c.location(), b.location());
    assertEquals(c.city(), b.city());
    assertEquals(c.key(), b.key());
  }

  public void testDecodeEmptiesByteBuffer() {
    ProtobufCodec<TestPerson> e = CodecFactory.encoder(TestPerson.class);
    ByteBuffer buf = ByteBuffer.wrap(testingBin);
    TestPerson p = e.decode(buf);
    assertEquals(0, buf.remaining());
    assertEquals(testingBin.length, buf.position());
  }

  public void testEncodeFillsByteBuffer() throws UnsupportedEncodingException {
    ProtobufCodec<TestPerson> e = CodecFactory.encoder(TestPerson.class);

    TestPerson p = new TestPerson(new TestPerson.Key("testing"), 75);
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

  public void testEncodeNonArrayByteBuffer()
      throws UnsupportedEncodingException {
    ProtobufCodec<TestPerson> e = CodecFactory.encoder(TestPerson.class);

    TestPerson p = new TestPerson(new TestPerson.Key("testing"), 75);
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

  private static String asString(byte[] bin)
      throws UnsupportedEncodingException {
    return new String(bin, "ISO-8859-1");
  }
}
