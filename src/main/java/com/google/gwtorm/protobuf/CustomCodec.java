// Copyright 2008 Google Inc.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identity of a custom {@link ProtobufCodec} for a {@code Column}.
 * <p>
 * Additional annotation tagged onto a {@code Column} field that carries the
 * name of a custom {@link ProtobufCodec} that should be used to handle that
 * field. The field data will be treated as an opaque binary sequence, so its
 * {@link ProtobufCodec#sizeof(Object)} method must be accurate.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CustomCodec {
  Class<? extends ProtobufCodec<?>> value();
}
