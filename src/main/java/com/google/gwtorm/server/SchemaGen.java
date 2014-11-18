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

import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.SequenceModel;
import com.google.gwtorm.schema.Util;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Generates a concrete implementation of a {@link Schema} extension. */
public class SchemaGen<S extends AbstractSchema> implements Opcodes {
  public interface AccessGenerator {
    Class<?> create(GeneratedClassLoader loader, RelationModel rm)
        throws OrmException;
  }

  private final GeneratedClassLoader classLoader;
  private final SchemaModel schema;
  private final Class<?> databaseClass;
  private final Class<S> schemaSuperClass;
  private final AccessGenerator accessGen;
  private List<RelationGen> relations;
  private ClassWriter cw;
  private String implClassName;
  private String implTypeName;

  public SchemaGen(final GeneratedClassLoader loader,
      final SchemaModel schemaModel, final Class<?> databaseType,
      final Class<S> superType, final AccessGenerator ag) {
    classLoader = loader;
    schema = schemaModel;
    databaseClass = databaseType;
    schemaSuperClass = superType;
    accessGen = ag;
  }

  public Class<Schema> create() throws OrmException {
    defineRelationClasses();

    init();
    implementRelationFields();
    implementConstructor();
    implementSequenceMethods();
    implementRelationMethods();
    implementAllRelationsMethod();

    cw.visitEnd();
    classLoader.defineClass(getImplClassName(), cw.toByteArray());
    return loadClass();
  }

  @SuppressWarnings("unchecked")
  private Class<Schema> loadClass() throws OrmException {
    try {
      final Class<?> c = Class.forName(getImplClassName(), false, classLoader);
      return (Class<Schema>) c;
    } catch (ClassNotFoundException err) {
      throw new OrmException("Cannot load generated class", err);
    }
  }

  String getSchemaClassName() {
    return schema.getSchemaClassName();
  }

  String getImplClassName() {
    return implClassName;
  }

  String getImplTypeName() {
    return implTypeName;
  }

  private void defineRelationClasses() throws OrmException {
    relations = new ArrayList<>();
    for (final RelationModel rel : schema.getRelations()) {
      final Class<?> a = accessGen.create(classLoader, rel);
      relations.add(new RelationGen(rel, a));
    }

    Collections.sort(relations, new Comparator<RelationGen>() {
      @Override
      public int compare(RelationGen a, RelationGen b) {
        int cmp = a.model.getRelationID() - b.model.getRelationID();
        if (cmp == 0) {
          cmp = a.model.getRelationName().compareTo(b.model.getRelationName());
        }
        return cmp;
      }
    });
  }

  private void init() {
    implClassName = getSchemaClassName() + "_Schema_" + Util.createRandomName();
    implTypeName = implClassName.replace('.', '/');

    cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(V1_3, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, implTypeName, null, Type
        .getInternalName(schemaSuperClass), new String[] {getSchemaClassName()
        .replace('.', '/')});
  }

  private void implementRelationFields() {
    for (final RelationGen info : relations) {
      info.implementField();
    }
  }

  private void implementConstructor() {
    final String consName = "<init>";
    final Type superType = Type.getType(schemaSuperClass);
    final Type dbType = Type.getType(databaseClass);

    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, consName, Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {dbType}), null, null);
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitMethodInsn(INVOKESPECIAL, superType.getInternalName(), consName,
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {Type
            .getType(schemaSuperClass.getDeclaredConstructors()[0]
                .getParameterTypes()[0])}));

    for (final RelationGen info : relations) {
      mv.visitVarInsn(ALOAD, 0);
      mv.visitTypeInsn(NEW, info.accessType.getInternalName());
      mv.visitInsn(DUP);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKESPECIAL, info.accessType.getInternalName(),
          consName, Type.getMethodDescriptor(Type.VOID_TYPE,
              new Type[] {superType}));
      mv.visitFieldInsn(PUTFIELD, implTypeName, info
          .getAccessInstanceFieldName(), info.accessType.getDescriptor());
    }

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementSequenceMethods() {
    for (final SequenceModel seq : schema.getSequences()) {
      final Type retType = Type.getType(seq.getResultType());
      final MethodVisitor mv =
          cw
              .visitMethod(ACC_PUBLIC, seq.getMethodName(), Type
                  .getMethodDescriptor(retType, new Type[] {}), null,
                  new String[] {Type.getType(OrmException.class)
                      .getInternalName()});
      mv.visitCode();

      mv.visitVarInsn(ALOAD, 0);
      mv.visitLdcInsn(seq.getSequenceName());
      mv.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(schemaSuperClass),
          "nextLong", Type.getMethodDescriptor(Type.getType(Long.TYPE),
              new Type[] {Type.getType(String.class)}));
      if (retType.getSize() == 1) {
        mv.visitInsn(L2I);
        mv.visitInsn(IRETURN);
      } else {
        mv.visitInsn(LRETURN);
      }
      mv.visitMaxs(-1, -1);
      mv.visitEnd();
    }
  }

  private void implementRelationMethods() {
    for (final RelationGen info : relations) {
      info.implementMethod();
    }
  }

  private void implementAllRelationsMethod() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "allRelations", Type
            .getMethodDescriptor(Type.getType(Access[].class), new Type[] {}),
            null, null);
    mv.visitCode();

    final int r = 1;
    CodeGenSupport cgs = new CodeGenSupport(mv);
    cgs.push(relations.size());
    mv.visitTypeInsn(ANEWARRAY, Type.getType(Access.class).getInternalName());
    mv.visitVarInsn(ASTORE, r);

    int index = 0;
    for (RelationGen info : relations) {
      mv.visitVarInsn(ALOAD, r);
      cgs.push(index++);

      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKEVIRTUAL, getImplTypeName(), info.model
          .getMethodName(), info.getDescriptor());

      mv.visitInsn(AASTORE);
    }

    mv.visitVarInsn(ALOAD, r);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private class RelationGen {
    final RelationModel model;
    final Type accessType;

    RelationGen(final RelationModel model, final Class<?> accessClass) {
      this.model = model;
      this.accessType = Type.getType(accessClass);
    }

    void implementField() {
      cw.visitField(ACC_PRIVATE | ACC_FINAL, getAccessInstanceFieldName(),
          accessType.getDescriptor(), null, null).visitEnd();
    }

    String getAccessInstanceFieldName() {
      return "access_" + model.getMethodName();
    }

    void implementMethod() {
      final MethodVisitor mv =
          cw.visitMethod(ACC_PUBLIC | ACC_FINAL, model.getMethodName(),
              getDescriptor(), null, null);
      mv.visitCode();
      mv.visitVarInsn(ALOAD, 0);
      mv.visitFieldInsn(GETFIELD, implTypeName, getAccessInstanceFieldName(),
          accessType.getDescriptor());
      mv.visitInsn(ARETURN);
      mv.visitMaxs(-1, -1);
      mv.visitEnd();
    }

    String getDescriptor() {
      return Type.getMethodDescriptor(Type.getObjectType(model
          .getAccessInterfaceName().replace('.', '/')), new Type[] {});
    }
  }
}
