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

import com.google.gwtorm.client.Column;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.nio.ByteBuffer;

/**
 * Encode and decode an arbitrary Java object as a Protobuf message.
 * <p>
 * The object must use the {@link Column} annotations to denote the fields that
 * should be encoded or decoded.
 */
public abstract class ProtobufCodec<T> {
  /** Encode the object into an immutable byte string. */
  public ByteString encodeToByteString(T obj) {
    return ByteString.copyFrom(encodeToByteBuffer(obj));
  }

  /** Encode the object into an immutable byte string. */
  public ByteBuffer encodeToByteBuffer(T obj) {
    ByteBuffer data = ByteBuffer.allocate(sizeof(obj));
    encode(obj, data);
    data.flip();
    return data;
  }

  /** Encode the object into a byte array. */
  public byte[] encodeToByteArray(T obj) {
    byte[] data = new byte[sizeof(obj)];
    encode(obj, data);
    return data;
  }

  /** Encode the object into a byte array. */
  public void encode(T obj, final byte[] data) {
    encode(obj, data, 0, data.length);
  }

  /** Encode the object into a byte array. */
  public void encode(T obj, final byte[] data, int offset, int length) {
    encode(obj, CodedOutputStream.newInstance(data, offset, length));
  }

  /** Encode the object into a ByteBuffer. */
  public void encode(T obj, ByteBuffer buf) {
    if (buf.hasArray()) {
      CodedOutputStream out = CodedOutputStream.newInstance( //
          buf.array(), //
          buf.position(), //
          buf.remaining());
      encode(obj, out);
      buf.position(buf.position() + (buf.remaining() - out.spaceLeft()));

    } else {
      encode(obj, CodedOutputStream.newInstance(newStream(buf)));
    }
  }

  private static ByteBufferOutputStream newStream(ByteBuffer buf) {
    return new ByteBufferOutputStream(buf);
  }

  /** Encode the object to the supplied output stream. */
  protected abstract void encode(T obj, CodedOutputStream out);

  /** Compute the number of bytes of the encoded form of the object. */
  public abstract int sizeof(T obj);

  /** Decode a byte string into an object instance. */
  public T decode(ByteString buf) {
    return decode(buf.newCodedInput());
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data) {
    return decode(data, 0, data.length);
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data, int offset, int length) {
    return decode(CodedInputStream.newInstance(data, offset, length));
  }

  /** Decode a byte buffer into an object instance. */
  public T decode(ByteBuffer buf) {
    if (buf.hasArray()) {
      CodedInputStream in = CodedInputStream.newInstance( //
          buf.array(), //
          buf.position(), //
          buf.remaining());
      T obj = decode(in);
      buf.position(buf.position() + in.getTotalBytesRead());
      return obj;
    } else {
      return decode(ByteString.copyFrom(buf));
    }
  }

  /** Decode an object by reading it from the stream. */
  protected abstract T decode(CodedInputStream in);
}
