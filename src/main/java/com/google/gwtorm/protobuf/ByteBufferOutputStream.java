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

package com.google.gwtorm.protobuf;

import java.io.OutputStream;
import java.nio.ByteBuffer;

class ByteBufferOutputStream extends OutputStream {
  private final ByteBuffer buffer;

  ByteBufferOutputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void write(int b) {
    buffer.put((byte) b);
  }

  @Override
  public void write(byte[] src, int offset, int length) {
    buffer.put(src, offset, length);
  }
}
