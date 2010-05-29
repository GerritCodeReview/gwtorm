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

package com.google.gwtorm.server;

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.client.SchemaFactory;
import com.google.gwtorm.schema.Util;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** Generates a factory to efficiently create new Schema instances. */
public class SchemaConstructorGen<T extends Schema> implements Opcodes {
  private static final String CTX = "schemaArg";

  private final GeneratedClassLoader classLoader;
  private final Class<T> schemaImpl;
  private final Object schemaArg;
  private ClassWriter cw;
  private String implClassName;
  private String implTypeName;

  public SchemaConstructorGen(final GeneratedClassLoader loader,
      final Class<T> c, final Object f) {
    classLoader = loader;
    schemaImpl = c;
    schemaArg = f;
  }

  public void defineClass() throws OrmException {
    init();
    declareFactoryField();
    implementConstructor();
    implementNewInstance();
    cw.visitEnd();
    classLoader.defineClass(implClassName, cw.toByteArray());
  }


  public SchemaFactory<T> create() throws OrmException {
    defineClass();
    try {
      final Class<?> c = Class.forName(implClassName, true, classLoader);
      final Constructor<?> n = c.getDeclaredConstructors()[0];
      return cast(n.newInstance(new Object[] {schemaArg}));
    } catch (InstantiationException e) {
      throw new OrmException("Cannot create schema factory", e);
    } catch (IllegalAccessException e) {
      throw new OrmException("Cannot create schema factory", e);
    } catch (ClassNotFoundException e) {
      throw new OrmException("Cannot create schema factory", e);
    } catch (IllegalArgumentException e) {
      throw new OrmException("Cannot create schema factory", e);
    } catch (InvocationTargetException e) {
      throw new OrmException("Cannot create schema factory", e);
    }
  }

  @SuppressWarnings("unchecked")
  private SchemaFactory<T> cast(final Object newInstance) {
    return (SchemaFactory<T>) newInstance;
  }

  private void init() {
    implClassName =
        schemaImpl.getName() + "_Factory_" + Util.createRandomName();
    implTypeName = implClassName.replace('.', '/');

    cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(V1_3, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, implTypeName, null, Type
        .getInternalName(Object.class), new String[] {Type
        .getInternalName(SchemaFactory.class)});
  }

  private void declareFactoryField() {
    cw.visitField(ACC_PRIVATE | ACC_FINAL, CTX,
        Type.getType(schemaArg.getClass()).getDescriptor(), null, null)
        .visitEnd();
  }

  private void implementConstructor() {
    final Type ft = Type.getType(schemaArg.getClass());
    final String consName = "<init>";
    final String consDesc =
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {ft});
    final MethodVisitor mv;
    mv = cw.visitMethod(ACC_PUBLIC, consName, consDesc, null, null);
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, Type.getInternalName(Object.class),
        consName, Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitFieldInsn(PUTFIELD, implTypeName, CTX, ft.getDescriptor());

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementNewInstance() {
    final Type ft = Type.getType(schemaArg.getClass());
    final String typeName = Type.getType(schemaImpl).getInternalName();
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "open", Type
            .getMethodDescriptor(Type.getType(Schema.class), new Type[] {}),
            null, null);
    mv.visitCode();

    Constructor<?> c = schemaImpl.getDeclaredConstructors()[0];
    Type argType = Type.getType(c.getParameterTypes()[0]);

    mv.visitTypeInsn(NEW, typeName);
    mv.visitInsn(DUP);
    mv.visitVarInsn(ALOAD, 0);
    mv.visitFieldInsn(GETFIELD, implTypeName, CTX, ft.getDescriptor());
    mv.visitMethodInsn(INVOKESPECIAL, typeName, "<init>", Type
        .getMethodDescriptor(Type.VOID_TYPE, new Type[] {argType}));
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }
}
