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
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

  /**
   * Encodes the object, prefixed by its encoded length.
   * <p>
   * The length is encoded as a raw varint with no tag.
   *
   * @param obj the object to encode.
   * @param out stream that will receive the object's data.
   * @throws IOException the stream failed to write data.
   */
  public void encodeWithSize(T obj, OutputStream out) throws IOException {
    CodedOutputStream cos = CodedOutputStream.newInstance(out);
    cos.writeRawVarint32(sizeof(obj));
    encode(obj, cos);
    cos.flush();
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
    T obj = newInstance();
    mergeFrom(buf, obj);
    return obj;
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data) {
    T obj = newInstance();
    mergeFrom(data, obj);
    return obj;
  }

  /** Decode a byte array into an object instance. */
  public T decode(byte[] data, int offset, int length) {
    T obj = newInstance();
    mergeFrom(data, offset, length, obj);
    return obj;
  }

  /** Decode a byte buffer into an object instance. */
  public T decode(ByteBuffer buf) {
    T obj = newInstance();
    mergeFrom(buf, obj);
    return obj;
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

  /** Decode an object that is prefixed by its encoded length. */
  public T decodeWithSize(InputStream in) throws IOException {
    T obj = newInstance();
    mergeFromWithSize(in, obj);
    return obj;
  }

  /** Decode a byte string into an existing object instance. */
  public void mergeFrom(ByteString buf, T obj) {
    try {
      mergeFrom(buf.newCodedInput(), obj);
    } catch (IOException err) {
      throw new RuntimeException("Cannot decode message", err);
    }
  }

  /** Decode a byte array into an existing object instance. */
  public void mergeFrom(byte[] data, T obj) {
    mergeFrom(data, 0, data.length, obj);
  }

  /** Decode a byte array into an existing object instance. */
  public void mergeFrom(byte[] data, int offset, int length, T obj) {
    try {
      mergeFrom(CodedInputStream.newInstance(data, offset, length), obj);
    } catch (IOException err) {
      throw new RuntimeException("Cannot decode message", err);
    }
  }

  /** Decode a byte buffer into an existing object instance. */
  public void mergeFrom(ByteBuffer buf, T obj) {
    if (buf.hasArray()) {
      CodedInputStream in = CodedInputStream.newInstance( //
          buf.array(), //
          buf.position(), //
          buf.remaining());
      try {
        mergeFrom(in, obj);
      } catch (IOException err) {
        throw new RuntimeException("Cannot decode message", err);
      }
      buf.position(buf.position() + in.getTotalBytesRead());
    } else {
      mergeFrom(ByteString.copyFrom(buf), obj);
    }
  }

  /** Decode an object that is prefixed by its encoded length. */
  public void mergeFromWithSize(InputStream in, T obj) throws IOException {
    int sz = readRawVarint32(in);
    mergeFrom(CodedInputStream.newInstance(new CappedInputStream(in, sz)), obj);
  }

  /**
   * Decode an input stream into an existing object instance.
   *
   * @throws IOException the underlying stream cannot be read.
   */
  public abstract void mergeFrom(CodedInputStream in, T obj) throws IOException;

  private static int readRawVarint32(InputStream in) throws IOException {
    int b = in.read();
    if (b == -1) {
      throw new InvalidProtocolBufferException("Truncated input");
    }

    if ((b & 0x80) == 0) {
      return b;
    }

    int result = b & 0x7f;
    int offset = 7;
    for (; offset < 32; offset += 7) {
      b = in.read();
      if (b == -1) {
        throw new InvalidProtocolBufferException("Truncated input");
      }
      result |= (b & 0x7f) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }

    // Keep reading up to 64 bits.
    for (; offset < 64; offset += 7) {
      b = in.read();
      if (b == -1) {
        throw new InvalidProtocolBufferException("Truncated input");
      }
      if ((b & 0x80) == 0) {
        return result;
      }
    }

    throw new InvalidProtocolBufferException("Malformed varint");
  }
}
