// Copyright 2010 Google Inc.
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

package com.google.gwtorm.nosql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IndexKeyBuilderTest  {

  @Test
  public void testInt() {
    IndexKeyBuilder ib;

    ib = new IndexKeyBuilder();
    ib.add(0);
    assertEqualToBuilderResult(new byte[] {0x00}, ib);

    ib = new IndexKeyBuilder();
    ib.add(1);
    assertEqualToBuilderResult(new byte[] {0x01, 0x01}, ib);

    ib = new IndexKeyBuilder();
    ib.add(256);
    assertEqualToBuilderResult(new byte[] {0x02, 0x01, 0x00}, ib);
  }

  @Test
  public void testDelimiter() {
    IndexKeyBuilder ib = new IndexKeyBuilder();
    ib.delimiter();
    assertEqualToBuilderResult(new byte[] {0x00, 0x01}, ib);
  }

  @Test
  public void testStringASCII() {
    IndexKeyBuilder ib = new IndexKeyBuilder();
    ib.add("hi");
    assertEqualToBuilderResult(new byte[] {'h', 'i'}, ib);
  }

  @Test
  public void testStringNUL() {
    IndexKeyBuilder ib = new IndexKeyBuilder();
    ib.add("\0");
    assertEqualToBuilderResult(new byte[] {0x00, (byte) 0xff}, ib);
  }

  @Test
  public void testStringFF() {
    IndexKeyBuilder ib = new IndexKeyBuilder();
    ib.add(new byte[] {(byte) 0xff});
    assertEqualToBuilderResult(new byte[] {(byte) 0xff, 0x00}, ib);
  }

  @Test
  public void testInfinity() {
    IndexKeyBuilder ib = new IndexKeyBuilder();
    ib.infinity();
    assertEqualToBuilderResult(new byte[] {(byte) 0xff, (byte) 0xff}, ib);
  }

  private static void assertEqualToBuilderResult(byte[] exp, IndexKeyBuilder ic) {
    assertEquals(toString(exp), toString(ic.toByteArray()));
  }

  private static String toString(byte[] bin) {
    StringBuilder dst = new StringBuilder(bin.length * 2);
    for (byte b : bin) {
      dst.append(hexchar[(b >>> 4) & 0x0f]);
      dst.append(hexchar[b & 0x0f]);
    }
    return dst.toString();
  }

  private static final char[] hexchar =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', //
          'a', 'b', 'c', 'd', 'e', 'f'};
}
