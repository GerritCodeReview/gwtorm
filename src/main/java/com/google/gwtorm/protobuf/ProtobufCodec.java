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

import java.io.IOException;
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
    CodedOutputStream out = CodedOutputStream.newInstance(data, offset, length);
    try {
      encode(obj, out);
      out.flush();
    } catch (IOException err) {
      throw new RuntimeException("Cannot encode message", err);
    }
  }

  /** Encode the object into a ByteBuffer. */
  public void encode(T obj, ByteBuffer buf) {
    if (buf.hasArray()) {
      CodedOutputStream out = CodedOutputStream.newInstance( //
          buf.array(), //
          buf.position(), //
          buf.remaining());
      try {
        encode(obj, out);
        out.flush();
      } catch (IOException err) {
        throw new RuntimeException("Cannot encode message", err);
      }
      buf.position(buf.position() + (buf.remaining() - out.spaceLeft()));

    } else {
      CodedOutputStream out = CodedOutputStream.newInstance(newStream(buf));
      try {
        encode(obj, out);
        out.flush();
      } catch (IOException err) {
        throw new RuntimeException("Cannot encode message", err);
      }
    }
  }

  private static ByteBufferOutputStream newStream(ByteBuffer buf) {
    return new ByteBufferOutputStream(buf);
  }

  /**
   * Encode the object to the supplied output stream.
   * <p>
   * The stream {@code out} is not flushed by this method. Callers that need the
   * entire byte representation after invoking encode must flush the stream to
   * ensure its intermediate buffers have been written to the backing store.
   *
   * @param obj the object to encode.
   * @param out the stream to encode the object onto.
   * @throws IOException the underlying stream cannot be written to.
   */
  public abstract void encode(T obj, CodedOutputStream out) throws IOException;

  /** Compute the number of bytes of the encoded form of the object. */
  public abstract int sizeof(T obj);

  /** Create a new uninitialized instance of the object type. */
  public abstract T newInstance();

  /** Decode a byte string into an object instance. */
  public T decode(ByteString buf) {
    try {
      return decode(buf.newCodedInput());
    } catch (IOException err) {
      throw new RuntimeException("Cannot decode message", err);
    }
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data) {
    return decode(data, 0, data.length);
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data, int offset, int length) {
    try {
      return decode(CodedInputStream.newInstance(data, offset, length));
    } catch (IOException err) {
      throw new RuntimeException("Cannot decode message", err);
    }
  }

  /** Decode a byte buffer into an object instance. */
  public T decode(ByteBuffer buf) {
    if (buf.hasArray()) {
      CodedInputStream in = CodedInputStream.newInstance( //
          buf.array(), //
          buf.position(), //
          buf.remaining());
      T obj;
      try {
        obj = decode(in);
      } catch (IOException err) {
        throw new RuntimeException("Cannot decode message", err);
      }
      buf.position(buf.position() + in.getTotalBytesRead());
      return obj;
    } else {
      return decode(ByteString.copyFrom(buf));
    }
  }

  /**
   * Decode an object by reading it from the stream.
   *
   * @throws IOException the underlying stream cannot be read.
   */
  public T decode(CodedInputStream in) throws IOException {
    T obj = newInstance();
    mergeFrom(in, obj);
    return obj;
  }

  /**
   * Decode an input stream into an existing object instance.
   *
   * @throws IOException the underlying stream cannot be read.
   */
  public abstract void mergeFrom(CodedInputStream in, T obj) throws IOException;
}
