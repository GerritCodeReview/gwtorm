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

import java.io.IOException;
import java.io.InputStream;

class CappedInputStream extends InputStream {
  private final InputStream src;
  private int remaining;

  CappedInputStream(InputStream src, int limit) {
    this.src = src;
    this.remaining = limit;
  }

  @Override
  public int read() throws IOException {
    if (0 < remaining) {
      int r = src.read();
      if (r < 0) {
        remaining = 0;
      } else {
        remaining--;
      }
      return r;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    } else if (0 < remaining) {
      int n = src.read(b, off, Math.min(len, remaining));
      if (n < 0) {
        remaining = 0;
      } else {
        remaining -= n;
      }
      return n;
    } else {
      return -1;
    }
  }

  @Override
  public void close() throws IOException {
    remaining = 0;
    src.close();
  }
}
