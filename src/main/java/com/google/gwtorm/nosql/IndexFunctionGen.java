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

import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.QueryModel;
import com.google.gwtorm.schema.QueryParser;
import com.google.gwtorm.schema.Util;
import com.google.gwtorm.server.CodeGenSupport;
import com.google.gwtorm.server.GeneratedClassLoader;
import com.google.gwtorm.server.OrmException;

import org.antlr.runtime.tree.Tree;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Generates {@link IndexFunction} implementations. */
class IndexFunctionGen<T> implements Opcodes {
  private static final Type string = Type.getType(String.class);
  private static final Type object = Type.getType(Object.class);
  private static final Type indexKeyBuilder =
      Type.getType(IndexKeyBuilder.class);

  private final GeneratedClassLoader classLoader;
  private final QueryModel query;
  private final List<QueryModel.OrderBy> myFields;
  private final Class<T> pojo;
  private final Type pojoType;

  private ClassWriter cw;
  private String superTypeName;
  private String implClassName;
  private String implTypeName;

  IndexFunctionGen(final GeneratedClassLoader loader, final QueryModel qm,
      final Class<T> t) {
    classLoader = loader;
    query = qm;

    myFields = new ArrayList<QueryModel.OrderBy>();

    // Only add each parameter column once, but in the order used.
    // This avoids a range test on the same column from duplicating
    // the data in the index record.
    //
    for (ColumnModel m : leaves(query.getParameters())) {
      QueryModel.OrderBy o = new QueryModel.OrderBy(m, false);
      if (!myFields.contains(o)) {
        myFields.add(o);
      }
    }

    // Skip ORDER BY columns that match with the parameters, then
    // add anything else onto the end.
    //
    int p = 0;
    Iterator<QueryModel.OrderBy> orderby = orderByLeaves(query.getOrderBy()).iterator();
    while (p < myFields.size() && orderby.hasNext()) {
      QueryModel.OrderBy o = orderby.next();
      if (!myFields.get(p).equals(o)) {
        myFields.add(o);
        break;
      }
      p++;
    }
    while (orderby.hasNext()) {
      myFields.add(orderby.next());
    }

    pojo = t;
    pojoType = Type.getType(pojo);
  }

  private static List<ColumnModel> leaves(List<ColumnModel> in) {
    ArrayList<ColumnModel> r = new ArrayList<ColumnModel>(in.size());
    for (ColumnModel m : in) {
      if (m.isNested()) {
        r.addAll(m.getAllLeafColumns());
      } else {
        r.add(m);
      }
    }
    return r;
  }

  private static List<QueryModel.OrderBy> orderByLeaves(List<QueryModel.OrderBy> in) {
    ArrayList<QueryModel.OrderBy> r = new ArrayList<QueryModel.OrderBy>(in.size());
    for (QueryModel.OrderBy m : in) {
      if (m.column.isNested()) {
        for (ColumnModel c : m.column.getAllLeafColumns()) {
          r.add(new QueryModel.OrderBy(c, m.descending));
        }
      } else {
        r.add(m);
      }
    }
    return r;
  }

  IndexFunction<T> create() throws OrmException {
    init();
    implementConstructor();
    implementGetName();
    implementIncludes();
    implementEncode();
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

  @SuppressWarnings("unchecked")
  private static <T> IndexFunction<T> cast(final Object c) {
    return (IndexFunction<T>) c;
  }

  private void init() {
    superTypeName = Type.getInternalName(IndexFunction.class);
    implClassName =
        pojo.getName() + "_IndexFunction_" + query.getName() + "_"
            + Util.createRandomName();
    implTypeName = implClassName.replace('.', '/');

    cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(V1_3, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, implTypeName, null,
        superTypeName, new String[] {});
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

  private void implementGetName() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "getName", Type
            .getMethodDescriptor(Type.getType(String.class), new Type[] {}),
            null, null);
    mv.visitCode();
    mv.visitLdcInsn(query.getName());
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementIncludes() throws OrmException {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "includes", Type.getMethodDescriptor(
            Type.BOOLEAN_TYPE, new Type[] {object}), null, null);
    mv.visitCode();
    final IncludeCGS cgs = new IncludeCGS(mv);
    cgs.setEntityType(pojoType);

    mv.visitVarInsn(ALOAD, 1);
    mv.visitTypeInsn(CHECKCAST, pojoType.getInternalName());
    mv.visitVarInsn(ASTORE, 1);

    Set<ColumnModel> checked = new HashSet<ColumnModel>();
    for (QueryModel.OrderBy orderby : myFields) {
      checkNotNullFields(Collections.singleton(orderby.column), checked, mv, cgs);
    }

    final Tree parseTree = query.getParseTree();
    if (parseTree != null) {
      checkConstants(parseTree, mv, cgs);
    }

    cgs.push(1);
    mv.visitInsn(IRETURN);

    mv.visitLabel(cgs.no);
    cgs.push(0);
    mv.visitInsn(IRETURN);

    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private static void checkNotNullFields(Collection<ColumnModel> myFields,
      Set<ColumnModel> checked, MethodVisitor mv, IncludeCGS cgs)
      throws OrmException {
    for (ColumnModel f : myFields) {
      if (f.isNested()) {
        checkNotNullFields(f.getNestedColumns(), checked, mv, cgs);
      } else {
        checkNotNullScalar(mv, checked, cgs, f);
      }
    }
  }

  private static void checkNotNullScalar(MethodVisitor mv,
      Set<ColumnModel> checked, IncludeCGS cgs, ColumnModel f)
      throws OrmException {
    checkParentNotNull(f.getParent(), checked, mv, cgs);
    cgs.setFieldReference(f);

    switch (Type.getType(f.getPrimitiveType()).getSort()) {
      case Type.BOOLEAN:
      case Type.BYTE:
      case Type.SHORT:
      case Type.CHAR:
      case Type.INT:
      case Type.LONG:
        break;

      case Type.ARRAY:
      case Type.OBJECT: {
        if (f.getPrimitiveType() == byte[].class) {
          cgs.pushFieldValue();
          mv.visitJumpInsn(IFNULL, cgs.no);

        } else if (f.getPrimitiveType() == String.class) {
          cgs.pushFieldValue();
          mv.visitJumpInsn(IFNULL, cgs.no);

        } else if (f.getPrimitiveType() == java.sql.Timestamp.class
            || f.getPrimitiveType() == java.util.Date.class
            || f.getPrimitiveType() == java.sql.Date.class) {
          cgs.pushFieldValue();
          mv.visitJumpInsn(IFNULL, cgs.no);

        } else {
          throw new OrmException("Type " + f.getPrimitiveType()
              + " not supported for field " + f.getPathToFieldName());
        }
        break;
      }

      default:
        throw new OrmException("Type " + f.getPrimitiveType()
            + " not supported for field " + f.getPathToFieldName());
    }
  }

  private static void checkParentNotNull(ColumnModel f,
      Set<ColumnModel> checked, MethodVisitor mv, IncludeCGS cgs) {
    if (f != null && checked.add(f)) {
      checkParentNotNull(f.getParent(), checked, mv, cgs);
      cgs.setFieldReference(f);
      cgs.pushFieldValue();
      mv.visitJumpInsn(IFNULL, cgs.no);
    }
  }

  private void checkConstants(Tree node, MethodVisitor mv, IncludeCGS cgs)
      throws OrmException {
    switch (node.getType()) {
      // These don't impact the constant evaluation
      case QueryParser.ORDER:
      case QueryParser.LIMIT:
        break;

      case 0: // nil node used to join other nodes together
      case QueryParser.WHERE:
      case QueryParser.AND:
        for (int i = 0; i < node.getChildCount(); i++) {
          checkConstants(node.getChild(i), mv, cgs);
        }
        break;

      case QueryParser.LT:
      case QueryParser.LE:
      case QueryParser.GT:
      case QueryParser.GE:
      case QueryParser.EQ: {
        final Tree lhs = node.getChild(0);
        final Tree rhs = node.getChild(1);
        if (lhs.getType() != QueryParser.ID) {
          throw new OrmException("Unsupported query token");
        }

        cgs.setFieldReference(((QueryParser.Column) lhs).getField());
        switch (rhs.getType()) {
          case QueryParser.PLACEHOLDER:
            // Parameter evaluated at runtime
            break;

          case QueryParser.TRUE:
            cgs.pushFieldValue();
            mv.visitJumpInsn(IFEQ, cgs.no);
            break;

          case QueryParser.FALSE:
            cgs.pushFieldValue();
            mv.visitJumpInsn(IFNE, cgs.no);
            break;

          case QueryParser.CONSTANT_INTEGER:
            cgs.pushFieldValue();
            cgs.push(Integer.parseInt(rhs.getText()));
            mv.visitJumpInsn(IF_ICMPNE, cgs.no);
            break;

          case QueryParser.CONSTANT_STRING:
            if (cgs.getFieldReference().getPrimitiveType() == Character.TYPE) {
              cgs.push(dequote(rhs.getText()).charAt(0));
              cgs.pushFieldValue();
              mv.visitJumpInsn(IF_ICMPNE, cgs.no);
            } else {
              mv.visitLdcInsn(dequote(rhs.getText()));
              cgs.pushFieldValue();
              mv.visitMethodInsn(INVOKEVIRTUAL, string.getInternalName(),
                  "equals", Type.getMethodDescriptor(Type.BOOLEAN_TYPE,
                      new Type[] {object}));
              mv.visitJumpInsn(IFEQ, cgs.no);
            }
            break;
        }
        break;
      }

      default:
        throw new OrmException("Unsupported query token " + node.toStringTree());
    }
  }

  private static String dequote(String text) {
    return text.substring(1, text.length() - 1);
  }

  private void implementEncode() throws OrmException {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, "encode", Type.getMethodDescriptor(
            Type.VOID_TYPE, new Type[] {indexKeyBuilder, object}), null, null);
    mv.visitCode();
    final EncodeCGS cgs = new EncodeCGS(mv);
    cgs.setEntityType(pojoType);

    mv.visitVarInsn(ALOAD, 2);
    mv.visitTypeInsn(CHECKCAST, pojoType.getInternalName());
    mv.visitVarInsn(ASTORE, 2);

    encodeFields(myFields, mv, cgs);

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  static void encodeFields(Collection<QueryModel.OrderBy> myFields,
      final MethodVisitor mv, final EncodeCGS cgs) throws OrmException {
    Iterator<QueryModel.OrderBy> i = myFields.iterator();
    while (i.hasNext()) {
      QueryModel.OrderBy f = i.next();
      encodeScalar(f, mv, cgs);
      if (i.hasNext()) {
        cgs.delimiter();
      }
    }
  }

  static void encodeField(QueryModel.OrderBy f, final MethodVisitor mv,
      final EncodeCGS cgs) throws OrmException {
    if (f.column.isNested()) {
      encodeFields(orderByLeaves(Collections.singletonList(f)), mv, cgs);
    } else {
      encodeScalar(f, mv, cgs);
    }
  }

  private static void encodeScalar(QueryModel.OrderBy f, final MethodVisitor mv,
      final EncodeCGS cgs) throws OrmException {
    String method = f.descending ? "desc" : "add";
    ColumnModel c = f.column;
    cgs.setFieldReference(c);

    switch (Type.getType(c.getPrimitiveType()).getSort()) {
      case Type.BOOLEAN:
      case Type.BYTE:
      case Type.SHORT:
      case Type.CHAR:
      case Type.INT:
        cgs.pushBuilder();
        cgs.pushFieldValue();
        mv.visitInsn(I2L);
        mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
            method, Type.getMethodDescriptor(Type.VOID_TYPE,
                new Type[] {Type.LONG_TYPE}));
        break;

      case Type.LONG:
        cgs.pushBuilder();
        cgs.pushFieldValue();
        mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
            method, Type.getMethodDescriptor(Type.VOID_TYPE,
                new Type[] {Type.LONG_TYPE}));
        break;

      case Type.ARRAY:
      case Type.OBJECT: {
        if (c.getPrimitiveType() == byte[].class) {
          cgs.pushBuilder();
          cgs.pushFieldValue();
          mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
              method, Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {Type
                  .getType(byte[].class)}));

        } else if (c.getPrimitiveType() == String.class) {
          cgs.pushBuilder();
          cgs.pushFieldValue();
          mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
              method, Type.getMethodDescriptor(Type.VOID_TYPE,
                  new Type[] {string}));

        } else if (c.getPrimitiveType() == java.sql.Timestamp.class
            || c.getPrimitiveType() == java.util.Date.class
            || c.getPrimitiveType() == java.sql.Date.class) {
          cgs.pushBuilder();
          cgs.pushFieldValue();
          String tsType = Type.getType(c.getPrimitiveType()).getInternalName();
          mv.visitMethodInsn(INVOKEVIRTUAL, tsType, "getTime", Type
              .getMethodDescriptor(Type.LONG_TYPE, new Type[] {}));
          mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
              method, Type.getMethodDescriptor(Type.VOID_TYPE,
                  new Type[] {Type.LONG_TYPE}));
        } else {
          throw new OrmException("Type " + c.getPrimitiveType()
              + " not supported for field " + c.getPathToFieldName());
        }
        break;
      }

      default:
        throw new OrmException("Type " + c.getPrimitiveType()
            + " not supported for field " + c.getPathToFieldName());
    }
  }

  private static final class IncludeCGS extends CodeGenSupport {
    final Label no = new Label();

    private IncludeCGS(MethodVisitor method) {
      super(method);
    }

    @Override
    public void pushEntity() {
      mv.visitVarInsn(ALOAD, 1);
    }
  }

  static class EncodeCGS extends CodeGenSupport {
    EncodeCGS(MethodVisitor method) {
      super(method);
    }

    void infinity() {
      pushBuilder();
      mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
          "infinity", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
    }

    void delimiter() {
      pushBuilder();
      mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
          "delimiter", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
    }

    void nul() {
      pushBuilder();
      mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
          "nul", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
    }

    void pushBuilder() {
      mv.visitVarInsn(ALOAD, 1);
    }

    @Override
    public void pushEntity() {
      mv.visitVarInsn(ALOAD, 2);
    }
  }
}
