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

package com.google.gwtorm.nosql.generic;

import com.google.gwtorm.nosql.IndexRow;

class CandidateRow {
  private final byte[] indexKey;
  private final IndexRow indexRow;
  private byte[] objData;

  CandidateRow(byte[] ik, IndexRow ir) {
    indexKey = ik;
    indexRow = ir;
    objData = indexRow.getDataCopy();
  }

  byte[] getIndexKey() {
    return indexKey;
  }

  IndexRow getIndexRow() {
    return indexRow;
  }

  byte[] getDataKey() {
    return indexRow.getDataKey();
  }

  boolean hasData() {
    return objData != null;
  }

  byte[] getData() {
    return objData;
  }

  void setData(byte[] objData) {
    this.objData = objData;
  }
}
