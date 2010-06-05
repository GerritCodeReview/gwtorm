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


import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Encoder support for {@link IndexFunction} computed strings.
 * <p>
 * This class provides a string that may contain multiple values, using
 * delimiters between fields and big-endian encoded numerics. Sorting the
 * resulting strings using unsigned byte orderings produces a stable sorting.
 * <p>
 * The encoding used by this class relies on having 258 tokens. To get the extra
 * 2 tokens within a 256 byte range, escapes are used according to the following
 * simple table:
 * <ul>
 * <li>delimiter = \x00\x01
 * <li>byte \x00 = \x00\xff
 * <li>byte \xff = \xff\x00
 * <li>infinity = \xff\xff
 * </ul>
 * <p>
 * Integers are encoded as variable length big-endian values, skipping leading
 * zero bytes, prefixed by the number of bytes used to encode them. Therefore 0
 * is encoded as "\x00", and 256 is encoded as "\x02\x01\x00". Negative values
 * are encoded in their twos complement encoding and therefore sort after the
 * maximum positive value.
 * <p>
 * Strings and byte arrays supplied by the caller have their \x00 and \xff
 * values escaped according to the table above, but are otherwise written as-is
 * without a length prefix.
 * <p>
 * Callers are responsible for inserting {@link #delimiter()} markers at the
 * appropriate positions in the sequence.
 */
public class IndexKeyBuilder {
  private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

  /**
   * Add a delimiter marker to the string.
   */
  public void delimiter() {
    buf.write(0x00);
    buf.write(0x01);
  }

  /**
   * Add the special infinity symbol to the string.
   * <p>
   * The infinity symbol sorts after all other values in the same position.
   */
  public void infinity() {
    buf.write(0xff);
    buf.write(0xff);
  }

  /**
   * Add \0 to the string.
   * <p>
   * \0 can be used during searches to enforce greater then or less then clauses
   * in a query.
   */
  public void nul() {
    buf.write(0x00);
  }

  /**
   * Add a raw sequence of bytes.
   * <p>
   * The bytes 0x00 and 0xff are escaped by this method according to the
   * encoding table described in the class documentation.
   *
   * @param bin array to copy from.
   * @param pos first index to copy.
   * @param cnt number of bytes to copy.
   */
  public void add(byte[] bin, int pos, int cnt) {
    while (0 < cnt--) {
      byte b = bin[pos++];
      if (b == 0x00) {
        buf.write(0x00);
        buf.write(0xff);

      } else if (b == -1) {
        buf.write(0xff);
        buf.write(0x00);

      } else {
        buf.write(b);
      }
    }
  }

  /**
   * Add a raw sequence of bytes.
   * <p>
   * The bytes 0x00 and 0xff are escaped by this method according to the
   * encoding table described in the class documentation.
   *
   * @param bin the complete array to copy.
   */
  public void add(byte[] bin) {
    add(bin, 0, bin.length);
  }

  /**
   * Encode a string into UTF-8 and append as a sequence of bytes.
   *
   * @param str the string to encode and append.
   */
  public void add(String str) {
    try {
      add(str.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("JVM does not support UTF-8", e);
    }
  }

  /**
   * Add a single character as though it were part of a UTF-8 string.
   *
   * @param ch the character to encode and append.
   */
  public void add(char ch) {
    if (ch == 0x00) {
      buf.write(0x00);
      buf.write(0xff);

    } else if (ch <= 255) {
      buf.write(ch);

    } else {
      add(Character.toString(ch));
    }
  }

  /**
   * Add an integer value as a big-endian variable length integer.
   *
   * @param val the value to add.
   */
  public void add(long val) {
    final byte[] t = new byte[9];
    int i = t.length;
    while (val != 0) {
      t[--i] = (byte) (val & 0xff);
      val >>>= 8;
    }
    t[i - 1] = (byte) (t.length - i);
    buf.write(t, i - 1, t.length - i + 1);
  }

  /**
   * Add a byte array as-is, without escaping.
   * <p>
   * This should only be used the byte array came from a prior index key and the
   * caller is trying to create a new key with this key embedded at the end.
   *
   * @param bin the binary to append as-is, without further escaping.
   */
  public void addRaw(byte[] bin) {
    buf.write(bin, 0, bin.length);
  }

  /**
   * Obtain a copy of the internal storage array.
   *
   * @return the current state of this, converted into a flat byte array.
   */
  public byte[] toByteArray() {
    return buf.toByteArray();
  }
}
