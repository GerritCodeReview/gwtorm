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

import com.google.gwtorm.protobuf.ProtobufCodec;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import java.io.EOFException;
import java.io.IOException;

/** Encodes a relation number in front of an object. */
public class RelationCodec<T> extends ProtobufCodec<T> {
  /**
   * Pop the field number from the stream and return it.
   *
   * @param in the stream to pop the field number from. The caller is responsible for making sure
   *     the underlying stream had a mark set for at least 8 bytes so the tag can be examined,
   *     reset, and later read again during mergeFrom or decode.
   * @return the field number of the relation.
   * @throws IOException the stream cannot be read.
   */
  public static int peekId(CodedInputStream in) throws IOException {
    return in.readTag() >>> 3;
  }

  private final int fieldId;
  private final ProtobufCodec<T> objectCodec;

  public RelationCodec(int fieldId, ProtobufCodec<T> objectCodec) {
    this.fieldId = fieldId;
    this.objectCodec = objectCodec;
  }

  @Override
  public T newInstance() {
    return objectCodec.newInstance();
  }

  @Override
  public int sizeof(T obj) {
    int sz = objectCodec.sizeof(obj);
    return CodedOutputStream.computeTagSize(fieldId) //
        + CodedOutputStream.computeRawVarint32Size(sz) //
        + sz;
  }

  @Override
  public void encode(T obj, CodedOutputStream out) throws IOException {
    int sz = objectCodec.sizeof(obj);
    out.writeTag(fieldId, WireFormat.FieldType.MESSAGE.getWireType());
    out.writeRawVarint32(sz);
    objectCodec.encode(obj, out);
  }

  @Override
  public void mergeFrom(CodedInputStream in, T obj) throws IOException {
    boolean found = false;
    for (; ; ) {
      int tag = in.readTag();
      if (tag == 0) {
        if (found) {
          break;
        } else {
          // Reached EOF. But we require an object in our only field.
          throw new EOFException("Expected field " + fieldId);
        }
      }

      if ((tag >>> 3) == fieldId) {
        if ((tag & 0x7) == WireFormat.FieldType.MESSAGE.getWireType()) {
          int sz = in.readRawVarint32();
          int oldLimit = in.pushLimit(sz);
          objectCodec.mergeFrom(in, obj);
          in.checkLastTagWas(0);
          in.popLimit(oldLimit);
          found = true;
        } else {
          throw new InvalidProtocolBufferException(
              "Field " + fieldId + " should be length delimited (wire type 2)");
        }
      } else {
        in.skipField(tag);
      }
    }
  }
}
