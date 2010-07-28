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
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.Util;
import com.google.gwtorm.schema.java.JavaColumnModel;
import com.google.gwtorm.server.CodeGenSupport;
import com.google.gwtorm.server.GeneratedClassLoader;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

/** Generates {@link ProtobufCodec} implementations. */
class CodecGen<T> implements Opcodes {
  private static final Type illegalStateException =
      Type.getType(IllegalStateException.class);
  private static final Type collection =
      Type.getType(java.util.Collection.class);
  private static final Type iterator = Type.getType(java.util.Iterator.class);
  private static final Type string = Type.getType(String.class);
  private static final Type enumType = Type.getType(Enum.class);
  private static final Type byteString = Type.getType(ByteString.class);
  private static final Type object = Type.getType(Object.class);
  private static final Type codedOutputStream =
      Type.getType(CodedOutputStream.class);
  private static final Type codedInputStream =
      Type.getType(CodedInputStream.class);
  private final GeneratedClassLoader classLoader;
  private final Class<T> pojo;
  private final Type pojoType;

  private ClassWriter cw;
  private JavaColumnModel[] myFields;
  private String superTypeName;
  private String implClassName;
  private String implTypeName;

  private Map<Class<?>, NestedCodec> nestedCodecs;

  public CodecGen(final GeneratedClassLoader loader, final Class<T> t) {
    classLoader = loader;
    pojo = t;
    pojoType = Type.getType(pojo);
    nestedCodecs = new HashMap<Class<?>, NestedCodec>();
  }

  public ProtobufCodec<T> create() throws OrmException {
    myFields = scanFields(pojo);

    init();
    implementNewInstanceObject();
    implementNewInstanceSelf();

    implementSizeofObject();
    implementSizeofSelf();

    implementEncodeObject();
    implementEncodeSelf();

    implementMergeFromObject();
    implementMergeFromSelf();

    implementCodecFields();
    implementStaticInit();
    implementConstructor();
    cw.visitEnd();
    classLoader.defineClass(implClassName, cw.toByteArray());

    try {
      final Class<?> c = Class.forName(implClassName, true, classLoader);
      return cast(c.newInstance());
    } catch (InstantiationException e) {
      throw new OrmException("Cannot create new encoder", e);
    } catch (IllegalAccessException e) {
      throw new OrmException("Cannot create new encoder", e);
    } catch (ClassNotFoundException e) {
      throw new OrmException("Cannot create new encoder", e);
    }
  }

  private static JavaColumnModel[] scanFields(Class<?> in) throws OrmException {
    final Collection<JavaColumnModel> col = new ArrayList<JavaColumnModel>();
    while (in != null) {
      for (final Field f : in.getDeclaredFields()) {
        if (f.getAnnotation(Column.class) != null) {
          col.add(new JavaColumnModel(f));
        }
      }
      in = in.getSuperclass();
    }
    if (col.isEmpty()) {
      throw new OrmException(
          "Cannot create new encoder, no @Column fields found");
    }
    return sort(col);
  }

  private static JavaColumnModel[] sort(
      final Collection<? extends ColumnModel> col) {
    JavaColumnModel[] out = col.toArray(new JavaColumnModel[col.size()]);
    Arrays.sort(out, new Comparator<JavaColumnModel>() {
      @Override
      public int compare(JavaColumnModel o1, JavaColumnModel o2) {
        return o1.getColumnID() - o2.getColumnID();
      }
    });
    return out;
  }

  @SuppressWarnings("unchecked")
  private static <T> ProtobufCodec<T> cast(final Object c) {
    return (ProtobufCodec<T>) c;
  }

  private void init() {
    superTypeName = Type.getInternalName(ProtobufCodec.class);
    implClassName = pojo.getName() + "_protobuf_" + Util.createRandomName();
    implTypeName = implClassName.replace('.', '/');

    cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(V1_3, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, implTypeName, null,
        superTypeName, new String[] {});
  }

  private void implementCodecFields() {
    for (NestedCodec other : nestedCodecs.values()) {
      cw.visitField(ACC_PRIVATE | ACC_STATIC | ACC_FINAL, other.field,
          other.codecType.getDescriptor(), null, null).visitEnd();
    }
  }

  private void implementStaticInit() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_STATIC, "<clinit>", Type
            .getMethodDescriptor(Type.VOID_TYPE, new Type[] {}), null, null);
    mv.visitCode();

    for (NestedCodec other : nestedCodecs.values()) {
      mv.visitTypeInsn(NEW, other.codecType.getInternalName());
      mv.visitInsn(DUP);
      mv.visitMethodInsn(INVOKESPECIAL, other.codecType.getInternalName(),
          "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
      mv.visitFieldInsn(PUTSTATIC, implTypeName, other.field, other.codecType
          .getDescriptor());
    }

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementConstructor() {
    final String consName = "<init>";
    final String consDesc =
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {});
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, consName, consDesc, null, null);
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, superTypeName, consName, consDesc);

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementNewInstanceObject() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "newInstance", Type.getMethodDescriptor(
            object, new Type[] {}), null, new String[] {});
    mv.visitCode();

    mv.visitTypeInsn(NEW, pojoType.getInternalName());
    mv.visitInsn(DUP);
    mv.visitMethodInsn(INVOKESPECIAL, pojoType.getInternalName(), "<init>",
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));

    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementNewInstanceSelf() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "newInstance", Type.getMethodDescriptor(
            pojoType, new Type[] {}), null, new String[] {});
    mv.visitCode();

    mv.visitTypeInsn(NEW, pojoType.getInternalName());
    mv.visitInsn(DUP);
    mv.visitMethodInsn(INVOKESPECIAL, pojoType.getInternalName(), "<init>",
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));

    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementSizeofObject() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "sizeof", Type.getMethodDescriptor(
            Type.INT_TYPE, new Type[] {object}), null, new String[] {});
    mv.visitCode();
    final SizeofCGS cgs = new SizeofCGS(mv);
    cgs.sizeVar = cgs.newLocal();
    cgs.setEntityType(pojoType);

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitTypeInsn(CHECKCAST, pojoType.getInternalName());
    mv.visitMethodInsn(INVOKEVIRTUAL, implTypeName, "sizeof", Type
        .getMethodDescriptor(Type.INT_TYPE, new Type[] {pojoType}));

    mv.visitInsn(IRETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementSizeofSelf() throws OrmException {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "sizeof", Type.getMethodDescriptor(
            Type.INT_TYPE, new Type[] {pojoType}), null, new String[] {});
    mv.visitCode();
    final SizeofCGS cgs = new SizeofCGS(mv);
    cgs.sizeVar = cgs.newLocal();
    cgs.setEntityType(pojoType);

    cgs.push(0);
    mv.visitVarInsn(ISTORE, cgs.sizeVar);
    sizeofMessage(myFields, mv, cgs);

    mv.visitVarInsn(ILOAD, cgs.sizeVar);
    mv.visitInsn(IRETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void sizeofMessage(final JavaColumnModel[] myFields,
      final MethodVisitor mv, final SizeofCGS cgs) throws OrmException {
    for (final JavaColumnModel f : myFields) {
      if (f.isNested()) {
        final NestedCodec n = nestedFor(f);
        final Label end = new Label();
        cgs.setFieldReference(f);
        cgs.pushFieldValue();
        mv.visitJumpInsn(IFNULL, end);

        final int msgSizeVar = cgs.newLocal();
        mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
            .getDescriptor());
        cgs.pushFieldValue();
        mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
            "sizeof", Type.getMethodDescriptor(Type.INT_TYPE,
                new Type[] {n.pojoType}));
        mv.visitVarInsn(ISTORE, msgSizeVar);

        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.doinc("computeTagSize", Type.INT_TYPE);

        cgs.preinc();
        mv.visitVarInsn(ILOAD, msgSizeVar);
        cgs.doinc("computeRawVarint32Size", Type.INT_TYPE);

        cgs.preinc();
        mv.visitVarInsn(ILOAD, msgSizeVar);
        cgs.doinc();

        cgs.freeLocal(msgSizeVar);
        mv.visitLabel(end);

      } else if (f.isCollection()) {
        sizeofCollection(f, mv, cgs);

      } else {
        sizeofScalar(mv, cgs, f);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private NestedCodec nestedFor(JavaColumnModel f) {
    Class clazz = f.getNestedClass();
    NestedCodec n = nestedCodecs.get(clazz);
    if (n == null) {
      Class<? extends ProtobufCodec> codec = null;
      Type type = Type.getType(clazz);
      if (f.getField() != null) {
        final CustomCodec cc = f.getField().getAnnotation(CustomCodec.class);
        if (cc != null) {
          codec = cc.value();
          type = object;
        }
      }
      if (codec == null) {
        codec = CodecFactory.encoder(clazz).getClass();
      }

      n = new NestedCodec("codec" + f.getColumnID(), codec, type);
      nestedCodecs.put(clazz, n);
    }
    return n;
  }

  private void sizeofCollection(final JavaColumnModel f,
      final MethodVisitor mv, final SizeofCGS cgs) throws OrmException {
    final int itr = cgs.newLocal();
    final int val = cgs.newLocal();
    final Class<?> valClazz = (Class<?>) f.getArgumentTypes()[0];
    final Type valType = Type.getType(valClazz);
    final JavaColumnModel col = collectionColumn(f, valClazz);
    final SizeofCGS ng = new SizeofCGS(mv) {
      {
        sizeVar = cgs.sizeVar;
        setEntityType(valType);
      }

      @Override
      public void pushEntity() {
        mv.visitVarInsn(ALOAD, val);
      }

      @Override
      protected void appendGetField(final ColumnModel c) {
        if (c != col) {
          super.appendGetField(c);
        }
      }

      @Override
      public int newLocal() {
        return cgs.newLocal();
      }

      @Override
      public void freeLocal(int index) {
        cgs.freeLocal(index);
      }
    };

    final Label end = new Label();
    cgs.setFieldReference(f);
    cgs.pushFieldValue();
    mv.visitJumpInsn(IFNULL, end);

    cgs.setFieldReference(f);
    cgs.pushFieldValue();
    mv.visitMethodInsn(INVOKEINTERFACE, collection.getInternalName(),
        "iterator", Type.getMethodDescriptor(iterator, new Type[] {}));
    mv.visitVarInsn(ASTORE, itr);

    final Label doloop = new Label();
    mv.visitLabel(doloop);
    mv.visitVarInsn(ALOAD, itr);
    mv.visitMethodInsn(INVOKEINTERFACE, iterator.getInternalName(), "hasNext",
        Type.getMethodDescriptor(Type.BOOLEAN_TYPE, new Type[] {}));
    mv.visitJumpInsn(IFEQ, end);

    mv.visitVarInsn(ALOAD, itr);
    mv.visitMethodInsn(INVOKEINTERFACE, iterator.getInternalName(), "next",
        Type.getMethodDescriptor(object, new Type[] {}));
    mv.visitTypeInsn(CHECKCAST, valType.getInternalName());
    mv.visitVarInsn(ASTORE, val);

    sizeofMessage(new JavaColumnModel[] {col}, mv, ng);
    mv.visitJumpInsn(GOTO, doloop);

    mv.visitLabel(end);
    cgs.freeLocal(itr);
    cgs.freeLocal(val);
  }

  private JavaColumnModel collectionColumn(final JavaColumnModel f,
      final Class<?> valClazz) throws OrmException {
    return new JavaColumnModel( //
        f.getField(), //
        f.getPathToFieldName(), //
        f.getColumnID(), //
        valClazz);
  }

  private void sizeofScalar(final MethodVisitor mv, final SizeofCGS cgs,
      final JavaColumnModel f) throws OrmException {
    cgs.setFieldReference(f);

    switch (Type.getType(f.getPrimitiveType()).getSort()) {
      case Type.BOOLEAN:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeBoolSize", Type.INT_TYPE, Type.BOOLEAN_TYPE);
        break;

      case Type.CHAR:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeUInt32Size", Type.INT_TYPE, Type.INT_TYPE);
        break;

      case Type.BYTE:
      case Type.SHORT:
      case Type.INT:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeSInt32Size", Type.INT_TYPE, Type.INT_TYPE);
        break;

      case Type.FLOAT:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeFloatSize", Type.INT_TYPE, Type.FLOAT_TYPE);
        break;

      case Type.DOUBLE:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeDoubleSize", Type.INT_TYPE, Type.DOUBLE_TYPE);
        break;

      case Type.LONG:
        cgs.preinc();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.doinc("computeSInt64Size", Type.INT_TYPE, Type.LONG_TYPE);
        break;

      case Type.ARRAY:
      case Type.OBJECT: {
        final Label end = new Label();
        cgs.pushFieldValue();
        mv.visitJumpInsn(IFNULL, end);

        if (f.getPrimitiveType() == byte[].class) {
          cgs.preinc();
          cgs.push(f.getColumnID());
          cgs.doinc("computeTagSize", Type.INT_TYPE);

          cgs.preinc();
          cgs.pushFieldValue();
          mv.visitInsn(ARRAYLENGTH);
          cgs.doinc("computeRawVarint32Size", Type.INT_TYPE);

          cgs.preinc();
          cgs.pushFieldValue();
          mv.visitInsn(ARRAYLENGTH);
          cgs.doinc();

        } else if (f.getPrimitiveType() == String.class) {
          cgs.preinc();
          cgs.push(f.getColumnID());
          cgs.pushFieldValue();
          cgs.doinc("computeStringSize", Type.INT_TYPE, string);

        } else if (f.getPrimitiveType() == java.sql.Timestamp.class
            || f.getPrimitiveType() == java.util.Date.class
            || f.getPrimitiveType() == java.sql.Date.class) {
          cgs.preinc();
          cgs.push(f.getColumnID());
          cgs.pushFieldValue();
          String tsType = Type.getType(f.getPrimitiveType()).getInternalName();
          mv.visitMethodInsn(INVOKEVIRTUAL, tsType, "getTime", Type
              .getMethodDescriptor(Type.LONG_TYPE, new Type[] {}));
          cgs.doinc("computeFixed64Size", Type.INT_TYPE, Type.LONG_TYPE);

        } else if (f.getPrimitiveType().isEnum()) {
          cgs.preinc();
          cgs.push(f.getColumnID());
          cgs.pushFieldValue();
          mv.visitMethodInsn(INVOKEVIRTUAL, enumType.getInternalName(),
              "ordinal", //
              Type.getMethodDescriptor(Type.INT_TYPE, new Type[] {}));
          cgs.doinc("computeEnumSize", Type.INT_TYPE, Type.INT_TYPE);

        } else {
          throw new OrmException("Type " + f.getPrimitiveType()
              + " not supported for field " + f.getPathToFieldName());
        }
        mv.visitLabel(end);
        break;
      }

      default:
        throw new OrmException("Type " + f.getPrimitiveType()
            + " not supported for field " + f.getPathToFieldName());
    }
  }

  private void implementEncodeObject() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "encode", Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {object, codedOutputStream}), null,
            new String[] {});
    mv.visitCode();
    final EncodeCGS cgs = new EncodeCGS(mv);
    cgs.setEntityType(pojoType);

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitTypeInsn(CHECKCAST, pojoType.getInternalName());
    mv.visitVarInsn(ALOAD, 2);
    mv.visitMethodInsn(INVOKEVIRTUAL, implTypeName, "encode", Type
        .getMethodDescriptor(Type.VOID_TYPE, new Type[] {pojoType,
            codedOutputStream}));

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementEncodeSelf() throws OrmException {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "encode", Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {pojoType, codedOutputStream}), null,
            new String[] {});
    mv.visitCode();
    final EncodeCGS cgs = new EncodeCGS(mv);
    cgs.setEntityType(pojoType);

    encodeMessage(myFields, mv, cgs);

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void encodeMessage(final JavaColumnModel[] myFields,
      final MethodVisitor mv, final EncodeCGS cgs) throws OrmException {
    for (final JavaColumnModel f : myFields) {
      if (f.isNested()) {
        final NestedCodec n = nestedFor(f);

        final Label end = new Label();
        cgs.setFieldReference(f);
        cgs.pushFieldValue();
        mv.visitJumpInsn(IFNULL, end);

        final int msgSizeVar = cgs.newLocal();
        mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
            .getDescriptor());
        cgs.pushFieldValue();
        mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
            "sizeof", Type.getMethodDescriptor(Type.INT_TYPE,
                new Type[] {n.pojoType}));
        mv.visitVarInsn(ISTORE, msgSizeVar);

        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.push(WireFormat.FieldType.MESSAGE.getWireType());
        mv.visitMethodInsn(INVOKEVIRTUAL, codedOutputStream.getInternalName(),
            "writeTag", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
                Type.INT_TYPE, Type.INT_TYPE}));

        cgs.pushCodedOutputStream();
        mv.visitVarInsn(ILOAD, msgSizeVar);
        mv.visitMethodInsn(INVOKEVIRTUAL, codedOutputStream.getInternalName(),
            "writeRawVarint32", Type.getMethodDescriptor(Type.VOID_TYPE,
                new Type[] {Type.INT_TYPE}));

        mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
            .getDescriptor());
        cgs.pushFieldValue();
        cgs.pushCodedOutputStream();
        mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
            "encode", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
                n.pojoType, codedOutputStream}));

        cgs.freeLocal(msgSizeVar);
        mv.visitLabel(end);

      } else if (f.isCollection()) {
        encodeCollection(f, mv, cgs);

      } else {
        encodeScalar(mv, cgs, f);
      }
    }
  }

  private void encodeCollection(final JavaColumnModel f,
      final MethodVisitor mv, final EncodeCGS cgs) throws OrmException {
    final int itr = cgs.newLocal();
    final int val = cgs.newLocal();
    final Class<?> valClazz = (Class<?>) f.getArgumentTypes()[0];
    final Type valType = Type.getType(valClazz);
    final JavaColumnModel col = collectionColumn(f, valClazz);
    final EncodeCGS ng = new EncodeCGS(mv) {
      {
        sizeVar = cgs.sizeVar;
        setEntityType(valType);
      }

      @Override
      public void pushEntity() {
        mv.visitVarInsn(ALOAD, val);
      }

      @Override
      protected void appendGetField(final ColumnModel c) {
        if (c != col) {
          super.appendGetField(c);
        }
      }

      @Override
      public int newLocal() {
        return cgs.newLocal();
      }

      @Override
      public void freeLocal(int index) {
        cgs.freeLocal(index);
      }
    };

    final Label end = new Label();
    cgs.setFieldReference(f);
    cgs.pushFieldValue();
    mv.visitJumpInsn(IFNULL, end);

    cgs.setFieldReference(f);
    cgs.pushFieldValue();
    mv.visitMethodInsn(INVOKEINTERFACE, collection.getInternalName(),
        "iterator", Type.getMethodDescriptor(iterator, new Type[] {}));
    mv.visitVarInsn(ASTORE, itr);

    final Label doloop = new Label();
    mv.visitLabel(doloop);
    mv.visitVarInsn(ALOAD, itr);
    mv.visitMethodInsn(INVOKEINTERFACE, iterator.getInternalName(), "hasNext",
        Type.getMethodDescriptor(Type.BOOLEAN_TYPE, new Type[] {}));
    mv.visitJumpInsn(IFEQ, end);

    mv.visitVarInsn(ALOAD, itr);
    mv.visitMethodInsn(INVOKEINTERFACE, iterator.getInternalName(), "next",
        Type.getMethodDescriptor(object, new Type[] {}));
    mv.visitTypeInsn(CHECKCAST, valType.getInternalName());
    mv.visitVarInsn(ASTORE, val);

    encodeMessage(new JavaColumnModel[] {col}, mv, ng);
    mv.visitJumpInsn(GOTO, doloop);

    mv.visitLabel(end);
    cgs.freeLocal(itr);
    cgs.freeLocal(val);
  }

  private void encodeScalar(final MethodVisitor mv, final EncodeCGS cgs,
      final JavaColumnModel f) throws OrmException {
    cgs.setFieldReference(f);

    switch (Type.getType(f.getPrimitiveType()).getSort()) {
      case Type.BOOLEAN:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeBool", Type.BOOLEAN_TYPE);
        break;

      case Type.CHAR:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeUInt32", Type.INT_TYPE);
        break;

      case Type.BYTE:
      case Type.SHORT:
      case Type.INT:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeSInt32", Type.INT_TYPE);
        break;

      case Type.FLOAT:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeFloat", Type.FLOAT_TYPE);
        break;

      case Type.DOUBLE:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeDouble", Type.DOUBLE_TYPE);
        break;

      case Type.LONG:
        cgs.pushCodedOutputStream();
        cgs.push(f.getColumnID());
        cgs.pushFieldValue();
        cgs.write("writeSInt64", Type.LONG_TYPE);
        break;

      case Type.ARRAY:
      case Type.OBJECT: {
        final Label end = new Label();
        cgs.pushFieldValue();
        mv.visitJumpInsn(IFNULL, end);

        if (f.getPrimitiveType() == byte[].class) {
          cgs.pushCodedOutputStream();
          cgs.push(f.getColumnID());
          cgs.push(WireFormat.FieldType.BYTES.getWireType());
          mv.visitMethodInsn(INVOKEVIRTUAL,
              codedOutputStream.getInternalName(), "writeTag", Type
                  .getMethodDescriptor(Type.VOID_TYPE, new Type[] {
                      Type.INT_TYPE, Type.INT_TYPE}));

          cgs.pushCodedOutputStream();
          cgs.pushFieldValue();
          mv.visitInsn(ARRAYLENGTH);
          mv.visitMethodInsn(INVOKEVIRTUAL,
              codedOutputStream.getInternalName(), "writeRawVarint32", Type
                  .getMethodDescriptor(Type.VOID_TYPE,
                      new Type[] {Type.INT_TYPE}));

          cgs.pushCodedOutputStream();
          cgs.pushFieldValue();
          mv.visitMethodInsn(INVOKEVIRTUAL,
              codedOutputStream.getInternalName(), "writeRawBytes", Type
                  .getMethodDescriptor(Type.VOID_TYPE, new Type[] {Type
                      .getType(byte[].class)}));

        } else {
          cgs.pushCodedOutputStream();
          cgs.push(f.getColumnID());
          cgs.pushFieldValue();

          if (f.getPrimitiveType() == String.class) {
            cgs.write("writeString", string);

          } else if (f.getPrimitiveType() == java.sql.Timestamp.class
              || f.getPrimitiveType() == java.util.Date.class
              || f.getPrimitiveType() == java.sql.Date.class) {
            String tsType =
                Type.getType(f.getPrimitiveType()).getInternalName();
            mv.visitMethodInsn(INVOKEVIRTUAL, tsType, "getTime", Type
                .getMethodDescriptor(Type.LONG_TYPE, new Type[] {}));
            cgs.write("writeFixed64", Type.LONG_TYPE);

          } else if (f.getPrimitiveType().isEnum()) {
            mv.visitMethodInsn(INVOKEVIRTUAL, enumType.getInternalName(),
                "ordinal", //
                Type.getMethodDescriptor(Type.INT_TYPE, new Type[] {}));
            cgs.write("writeEnum", Type.INT_TYPE);

          } else {
            throw new OrmException("Type " + f.getPrimitiveType()
                + " not supported for field " + f.getPathToFieldName());
          }
        }
        mv.visitLabel(end);
        break;
      }

      default:
        throw new OrmException("Type " + f.getPrimitiveType()
            + " not supported for field " + f.getPathToFieldName());
    }
  }

  private void implementMergeFromObject() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "mergeFrom", Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {codedInputStream, object}), null,
            new String[] {});
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitVarInsn(ALOAD, 2);
    mv.visitTypeInsn(CHECKCAST, pojoType.getInternalName());
    mv.visitMethodInsn(INVOKEVIRTUAL, implTypeName, "mergeFrom", Type
        .getMethodDescriptor(Type.VOID_TYPE, new Type[] {codedInputStream,
            pojoType}));

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementMergeFromSelf() throws OrmException {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "mergeFrom", Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {codedInputStream, pojoType}), null,
            new String[] {});
    mv.visitCode();
    final DecodeCGS cgs = new DecodeCGS(mv);
    cgs.objVar = 2;
    cgs.tagVar = cgs.newLocal();
    cgs.setEntityType(pojoType);

    decodeMessage(myFields, mv, cgs);

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void decodeMessage(final JavaColumnModel[] myFields,
      final MethodVisitor mv, final DecodeCGS cgs) throws OrmException {
    final Label nextField = new Label();
    final Label end = new Label();
    mv.visitLabel(nextField);

    // while (!ci.isAtEnd) { ...
    cgs.call("readTag", Type.INT_TYPE);
    mv.visitInsn(DUP);
    mv.visitVarInsn(ISTORE, cgs.tagVar);

    cgs.push(3);
    mv.visitInsn(IUSHR);

    final Label badField = new Label();
    final int[] caseTags = new int[1 + myFields.length];
    final Label[] caseLabels = new Label[caseTags.length];

    caseTags[0] = 0;
    caseLabels[0] = new Label();

    int gaps = 0;
    for (int i = 1; i < caseTags.length; i++) {
      caseTags[i] = myFields[i - 1].getColumnID();
      caseLabels[i] = new Label();
      gaps += caseTags[i] - (caseTags[i - 1] + 1);
    }

    if (2 * gaps / 3 <= myFields.length) {
      final int min = 0;
      final int max = caseTags[caseTags.length - 1];
      final Label[] table = new Label[max + 1];
      Arrays.fill(table, badField);
      for (int idx = 0; idx < caseTags.length; idx++) {
        table[caseTags[idx]] = caseLabels[idx];
      }
      mv.visitTableSwitchInsn(min, max, badField, table);
    } else {
      mv.visitLookupSwitchInsn(badField, caseTags, caseLabels);
    }

    mv.visitLabel(caseLabels[0]);
    mv.visitJumpInsn(GOTO, end);

    for (int idx = 1; idx < caseTags.length; idx++) {
      final JavaColumnModel f = myFields[idx - 1];
      mv.visitLabel(caseLabels[idx]);
      decodeField(mv, cgs, f);
      mv.visitJumpInsn(GOTO, nextField);
    }

    // default:
    mv.visitLabel(badField);
    cgs.pushCodedInputStream();
    mv.visitVarInsn(ILOAD, cgs.tagVar);
    cgs.ncallInt("skipField", Type.BOOLEAN_TYPE);
    mv.visitInsn(POP);
    mv.visitJumpInsn(GOTO, nextField);

    mv.visitLabel(end);
    cgs.pushCodedInputStream();
    cgs.push(0);
    cgs.ncallInt("checkLastTagWas", Type.VOID_TYPE);
  }

  private void decodeField(final MethodVisitor mv, final DecodeCGS cgs,
      final JavaColumnModel f) throws OrmException {
    if (f.isNested()) {
      final NestedCodec n = nestedFor(f);
      final Label load = new Label();
      cgs.setFieldReference(f);
      cgs.pushFieldValue();
      mv.visitJumpInsn(IFNONNULL, load);

      // Since the field isn't initialized, construct it
      //
      cgs.fieldSetBegin();
      mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
          .getDescriptor());
      mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
          "newInstance", Type.getMethodDescriptor(n.pojoType, new Type[] {}));
      if (object.equals(n.pojoType)) {
        mv.visitTypeInsn(CHECKCAST, Type.getType(f.getNestedClass())
            .getInternalName());
      }
      cgs.fieldSetEnd();

      // read the length, set a new limit, decode the message, validate
      // we stopped at the end of it as expected.
      //
      mv.visitLabel(load);
      final int limitVar = cgs.newLocal();
      cgs.pushCodedInputStream();
      cgs.call("readRawVarint32", Type.INT_TYPE);
      cgs.ncallInt("pushLimit", Type.INT_TYPE);
      mv.visitVarInsn(ISTORE, limitVar);

      mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
          .getDescriptor());
      cgs.pushCodedInputStream();
      cgs.pushFieldValue();
      mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
          "mergeFrom", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
              codedInputStream, n.pojoType}));

      cgs.pushCodedInputStream();
      mv.visitVarInsn(ILOAD, limitVar);
      cgs.ncallInt("popLimit", Type.VOID_TYPE);
      cgs.freeLocal(limitVar);

    } else if (f.isCollection()) {
      decodeCollection(mv, cgs, f);

    } else {
      decodeScalar(mv, cgs, f);
    }
  }

  private void decodeCollection(final MethodVisitor mv, final DecodeCGS cgs,
      final JavaColumnModel f) throws OrmException {
    final Class<?> valClazz = (Class<?>) f.getArgumentTypes()[0];
    final Type valType = Type.getType(valClazz);
    final JavaColumnModel col = collectionColumn(f, valClazz);
    final DecodeCGS ng = new DecodeCGS(mv) {
      {
        tagVar = cgs.tagVar;
        setEntityType(valType);
      }

      @Override
      public int newLocal() {
        return cgs.newLocal();
      }

      @Override
      public void freeLocal(int index) {
        cgs.freeLocal(index);
      }

      @Override
      protected void appendGetField(final ColumnModel c) {
        if (c != col) {
          super.appendGetField(c);
        }
      }

      @Override
      public void fieldSetBegin() {
        if (col.isNested()) {
          super.fieldSetBegin();
        } else {
          cgs.pushFieldValue();
        }
      }

      @Override
      public void fieldSetEnd() {
        if (col.isNested()) {
          super.fieldSetEnd();
        } else {
          mv.visitMethodInsn(INVOKEINTERFACE, collection.getInternalName(),
              "add", Type.getMethodDescriptor(Type.BOOLEAN_TYPE,
                  new Type[] {object}));
          mv.visitInsn(POP);
        }
      }
    };

    final Label notnull = new Label();
    cgs.setFieldReference(f);
    cgs.pushFieldValue();
    mv.visitJumpInsn(IFNONNULL, notnull);

    // If the field is null, try to initialize it based on its declared type.
    // If we don't know what that is, we have to throw an exception instead.
    //
    final Type concreteType;
    if (!f.getNestedClass().isInterface()
        && (f.getNestedClass().getModifiers() & Modifier.ABSTRACT) == 0) {
      concreteType = Type.getType(f.getNestedClass());

    } else if (f.getNestedClass().isAssignableFrom(ArrayList.class)) {
      concreteType = Type.getType(ArrayList.class);

    } else if (f.getNestedClass().isAssignableFrom(HashSet.class)) {
      concreteType = Type.getType(HashSet.class);

    } else if (f.getNestedClass().isAssignableFrom(TreeSet.class)) {
      concreteType = Type.getType(TreeSet.class);

    } else {
      mv.visitTypeInsn(NEW, illegalStateException.getInternalName());
      mv.visitInsn(DUP);
      mv.visitLdcInsn("Field " + f.getPathToFieldName() + " not initialized");
      mv.visitMethodInsn(INVOKESPECIAL,
          illegalStateException.getInternalName(), "<init>", Type
              .getMethodDescriptor(Type.VOID_TYPE, new Type[] {string}));
      mv.visitInsn(ATHROW);
      concreteType = null;
    }
    if (concreteType != null) {
      cgs.fieldSetBegin();
      mv.visitTypeInsn(NEW, concreteType.getInternalName());
      mv.visitInsn(DUP);
      mv.visitMethodInsn(INVOKESPECIAL, concreteType.getInternalName(),
          "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
      cgs.fieldSetEnd();
    }
    mv.visitLabel(notnull);

    if (col.isNested()) {
      // If its nested, we have to build the object instance.
      //
      final NestedCodec n = nestedFor(col);
      ng.objVar = cgs.newLocal();
      mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
          .getDescriptor());
      mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
          "newInstance", Type.getMethodDescriptor(n.pojoType, new Type[] {}));
      mv.visitVarInsn(ASTORE, ng.objVar);

      // read the length, set a new limit, decode the message, validate
      // we stopped at the end of it as expected.
      //
      final int limitVar = cgs.newLocal();
      cgs.pushCodedInputStream();
      cgs.call("readRawVarint32", Type.INT_TYPE);
      cgs.ncallInt("pushLimit", Type.INT_TYPE);
      mv.visitVarInsn(ISTORE, limitVar);

      mv.visitFieldInsn(GETSTATIC, implTypeName, n.field, n.codecType
          .getDescriptor());
      cgs.pushCodedInputStream();
      mv.visitVarInsn(ALOAD, ng.objVar);
      mv.visitMethodInsn(INVOKEVIRTUAL, n.codecType.getInternalName(),
          "mergeFrom", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
              codedInputStream, n.pojoType}));

      cgs.pushCodedInputStream();
      mv.visitVarInsn(ILOAD, limitVar);
      cgs.ncallInt("popLimit", Type.VOID_TYPE);
      cgs.freeLocal(limitVar);
      cgs.pushFieldValue();

      mv.visitVarInsn(ALOAD, ng.objVar);
      mv.visitMethodInsn(INVOKEINTERFACE, collection.getInternalName(), "add",
          Type.getMethodDescriptor(Type.BOOLEAN_TYPE, new Type[] {object}));
      mv.visitInsn(POP);
      cgs.freeLocal(ng.objVar);

    } else if (col.isCollection()) {
      throw new OrmException("Cannot nest collection as member of another"
          + " collection: " + f.getPathToFieldName());

    } else {
      decodeScalar(mv, ng, col);
    }
  }

  private static void decodeScalar(final MethodVisitor mv, final DecodeCGS cgs,
      final JavaColumnModel f) throws OrmException {
    cgs.setFieldReference(f);
    cgs.fieldSetBegin();
    switch (Type.getType(f.getPrimitiveType()).getSort()) {
      case Type.BOOLEAN:
        cgs.call("readBool", Type.BOOLEAN_TYPE);
        break;

      case Type.CHAR:
        cgs.call("readUInt32", Type.INT_TYPE);
        break;

      case Type.BYTE:
      case Type.SHORT:
      case Type.INT:
        cgs.call("readSInt32", Type.INT_TYPE);
        break;

      case Type.FLOAT:
        cgs.call("readFloat", Type.FLOAT_TYPE);
        break;

      case Type.DOUBLE:
        cgs.call("readDouble", Type.DOUBLE_TYPE);
        break;

      case Type.LONG:
        cgs.call("readSInt64", Type.LONG_TYPE);
        break;

      default:
        if (f.getPrimitiveType() == byte[].class) {
          cgs.call("readBytes", byteString);
          mv.visitMethodInsn(INVOKEVIRTUAL, byteString.getInternalName(),
              "toByteArray", Type.getMethodDescriptor(Type
                  .getType(byte[].class), new Type[] {}));

        } else if (f.getPrimitiveType() == String.class) {
          cgs.call("readString", string);

        } else if (f.getPrimitiveType() == java.sql.Timestamp.class
            || f.getPrimitiveType() == java.util.Date.class
            || f.getPrimitiveType() == java.sql.Date.class) {
          String tsType = Type.getType(f.getPrimitiveType()).getInternalName();
          mv.visitTypeInsn(NEW, tsType);
          mv.visitInsn(DUP);
          cgs.call("readFixed64", Type.LONG_TYPE);
          mv.visitMethodInsn(INVOKESPECIAL, tsType, "<init>", //
              Type.getMethodDescriptor(Type.VOID_TYPE,
                  new Type[] {Type.LONG_TYPE}));

        } else if (f.getPrimitiveType().isEnum()) {
          Type et = Type.getType(f.getPrimitiveType());
          mv.visitMethodInsn(INVOKESTATIC, et.getInternalName(), "values", Type
              .getMethodDescriptor(Type.getType("[" + et.getDescriptor()),
                  new Type[] {}));
          cgs.call("readEnum", Type.INT_TYPE);
          mv.visitInsn(AALOAD);

        } else {
          throw new OrmException("Type " + f.getPrimitiveType()
              + " not supported for field " + f.getPathToFieldName());
        }
        break;
    }
    cgs.fieldSetEnd();
  }

  private static class SizeofCGS extends CodeGenSupport {
    int sizeVar;

    SizeofCGS(MethodVisitor method) {
      super(method);
    }

    void doinc(String name, Type... args) {
      mv.visitMethodInsn(INVOKESTATIC, codedOutputStream.getInternalName(),
          name, Type.getMethodDescriptor(Type.INT_TYPE, args));
      doinc();
    }

    void preinc() {
      mv.visitVarInsn(ILOAD, sizeVar);
    }

    void doinc() {
      mv.visitInsn(IADD);
      mv.visitVarInsn(ISTORE, sizeVar);
    }

    @Override
    public void pushEntity() {
      mv.visitVarInsn(ALOAD, 1);
    }
  }

  private static class EncodeCGS extends SizeofCGS {
    private EncodeCGS(MethodVisitor method) {
      super(method);
    }

    void pushCodedOutputStream() {
      mv.visitVarInsn(ALOAD, 2);
    }

    void write(String name, Type arg) {
      mv.visitMethodInsn(INVOKEVIRTUAL, codedOutputStream.getInternalName(),
          name, Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
              Type.INT_TYPE, arg}));
    }
  }

  private static class DecodeCGS extends CodeGenSupport {
    final int codedInputStreamVar = 1;
    int objVar;
    int tagVar;

    DecodeCGS(MethodVisitor method) {
      super(method);
    }

    void pushCodedInputStream() {
      mv.visitVarInsn(ALOAD, codedInputStreamVar);
    }

    void call(String name, Type ret) {
      pushCodedInputStream();
      mv.visitMethodInsn(INVOKEVIRTUAL, codedInputStream.getInternalName(),
          name, Type.getMethodDescriptor(ret, new Type[] {}));
    }

    void ncallInt(String name, Type ret) {
      mv.visitMethodInsn(INVOKEVIRTUAL, codedInputStream.getInternalName(),
          name, Type.getMethodDescriptor(ret, new Type[] {Type.INT_TYPE}));
    }

    @Override
    public void pushEntity() {
      mv.visitVarInsn(ALOAD, objVar);
    }
  }

  private static class NestedCodec {
    final String field;
    final Type codecType;
    final Type pojoType;

    @SuppressWarnings("unchecked")
    NestedCodec(String field, Class impl, Type pojoType) {
      this.field = field;
      this.codecType = Type.getType(impl);
      this.pojoType = pojoType;
    }
  }
}
