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

import com.google.gwtorm.client.Key;
import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.ResultSet;
import com.google.gwtorm.protobuf.CodecFactory;
import com.google.gwtorm.protobuf.ProtobufCodec;
import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.schema.KeyModel;
import com.google.gwtorm.schema.QueryModel;
import com.google.gwtorm.schema.QueryParser;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.Util;
import com.google.gwtorm.server.CodeGenSupport;
import com.google.gwtorm.server.GeneratedClassLoader;

import org.antlr.runtime.tree.Tree;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Generates a concrete implementation of a {@link NoSqlAccess} extension. */
class AccessGen implements Opcodes {
  private static final Type string = Type.getType(String.class);
  private static final Type protobufCodec = Type.getType(ProtobufCodec.class);
  private static final Type indexFunction = Type.getType(IndexFunction.class);
  private static final Type object = Type.getType(Object.class);
  private static final Type ormKey = Type.getType(Key.class);
  private static final Type byteArray = Type.getType(byte[].class);
  private static final Type ormException = Type.getType(OrmException.class);
  private static final Type resultSet = Type.getType(ResultSet.class);
  private static final Type indexKeyBuilder =
      Type.getType(IndexKeyBuilder.class);

  private static final String F_OBJECT_CODEC = "objectCodec";
  private static final String F_INDEXES = "indexes";

  private final GeneratedClassLoader classLoader;
  private final RelationModel model;
  private final Class<?> modelClass;
  private final Type schemaType;
  private final Type accessType;
  private final Type entityType;
  private final KeyModel key;

  private ClassWriter cw;
  private String implClassName;
  private String implTypeName;

  AccessGen(final GeneratedClassLoader loader, final RelationModel rm,
      final Class<? extends NoSqlSchema> schemaClazz,
      final Class<? extends NoSqlAccess> accessClazz) throws OrmException {
    classLoader = loader;
    model = rm;

    try {
      modelClass =
          Class.forName(model.getEntityTypeClassName(), true, classLoader);
    } catch (ClassNotFoundException cnfe) {
      throw new OrmException("Cannot locate model class", cnfe);
    }

    schemaType = Type.getType(schemaClazz);
    accessType = Type.getType(accessClazz);
    entityType = Type.getType(modelClass);

    key = model.getPrimaryKey();
    if (key == null) {
      throw new OrmException("Relation " + rm.getMethodName()
          + " has no primary key");
    }
  }

  Class<?> create() throws OrmException {
    init();
    implementStaticFields();
    implementConstructor();
    implementGetString("getRelationName", model.getRelationName());
    implementGetRelationID();
    implementGetObjectCodec();
    implementGetIndexes();

    implementPrimaryKey();
    implementEncodePrimaryKey();
    implementKeyQuery(key);

    for (final QueryModel q : model.getQueries()) {
      implementQuery(q);
    }
    implementQuery(new QueryModel(model, "iterateAllEntities", ""));

    cw.visitEnd();
    classLoader.defineClass(implClassName, cw.toByteArray());

    final Class<?> c = loadClass();
    initObjectCodec(c);
    initQueryIndexes(c);
    return c;
  }

  @SuppressWarnings("unchecked")
  private void initObjectCodec(final Class<?> clazz) throws OrmException {
    ProtobufCodec oc = CodecFactory.encoder(modelClass);
    if (model.getRelationID() > 0) {
      oc = new RelationCodec(model.getRelationID(), oc);
    }

    try {
      final Field e = clazz.getDeclaredField(F_OBJECT_CODEC);
      e.setAccessible(true);
      e.set(null, oc);
    } catch (IllegalArgumentException err) {
      throw new OrmException("Cannot setup ProtobufCodec", err);
    } catch (IllegalStateException err) {
      throw new OrmException("Cannot setup ProtobufCodec", err);
    } catch (IllegalAccessException err) {
      throw new OrmException("Cannot setup ProtobufCodec", err);
    } catch (SecurityException err) {
      throw new OrmException("Cannot setup ProtobufCodec", err);
    } catch (NoSuchFieldException err) {
      throw new OrmException("Cannot setup ProtobufCodec", err);
    }
  }

  @SuppressWarnings("unchecked")
  private void initQueryIndexes(final Class<?> clazz) throws OrmException {
    final Collection<QueryModel> queries = model.getQueries();
    final ArrayList<IndexFunction> indexes = new ArrayList<IndexFunction>();
    for (QueryModel m : queries) {
      if (needsIndexFunction(m)) {
        indexes.add(new IndexFunctionGen(classLoader, m, modelClass).create());
      }
    }

    try {
      Field e = clazz.getDeclaredField(F_INDEXES);
      e.setAccessible(true);
      e.set(null, indexes.toArray(new IndexFunction[indexes.size()]));

      for (IndexFunction f : indexes) {
        e = clazz.getDeclaredField("index_" + f.getName());
        e.setAccessible(true);
        e.set(null, f);
      }
    } catch (IllegalArgumentException err) {
      throw new OrmException("Cannot setup query IndexFunctions", err);
    } catch (IllegalStateException err) {
      throw new OrmException("Cannot setup query IndexFunctions", err);
    } catch (IllegalAccessException err) {
      throw new OrmException("Cannot setup query IndexFunctions", err);
    } catch (SecurityException err) {
      throw new OrmException("Cannot setup query IndexFunctions", err);
    } catch (NoSuchFieldException err) {
      throw new OrmException("Cannot setup query IndexFunctions", err);
    }
  }

  private Class<?> loadClass() throws OrmException {
    try {
      return Class.forName(implClassName, false, classLoader);
    } catch (ClassNotFoundException err) {
      throw new OrmException("Cannot load generated class", err);
    }
  }

  private void init() {
    implClassName =
        model.getEntityTypeClassName() + "_Access_" + model.getMethodName()
            + "_" + Util.createRandomName();
    implTypeName = implClassName.replace('.', '/');

    cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(V1_3, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, implTypeName, null,
        accessType.getInternalName(), new String[] {model
            .getAccessInterfaceName().replace('.', '/')});
  }

  private void implementStaticFields() {
    cw.visitField(ACC_PRIVATE | ACC_STATIC, F_OBJECT_CODEC,
        protobufCodec.getDescriptor(), null, null).visitEnd();
    cw.visitField(ACC_PRIVATE | ACC_STATIC, F_INDEXES,
        Type.getType(IndexFunction[].class).getDescriptor(), null, null)
        .visitEnd();

    for (final QueryModel q : model.getQueries()) {
      if (needsIndexFunction(q)) {
        cw.visitField(ACC_PRIVATE | ACC_STATIC, "index_" + q.getName(),
            indexFunction.getDescriptor(), null, null).visitEnd();
      }
    }
  }

  private void implementConstructor() {
    final String consName = "<init>";
    final String consDesc =
        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {schemaType});
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC, consName, consDesc, null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitMethodInsn(INVOKESPECIAL, accessType.getInternalName(), consName,
        consDesc);
    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementGetString(final String methodName,
      final String returnValue) {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, methodName, Type
            .getMethodDescriptor(string, new Type[] {}), null, null);
    mv.visitCode();
    mv.visitLdcInsn(returnValue);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementGetRelationID() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "getRelationID", Type
            .getMethodDescriptor(Type.INT_TYPE, new Type[] {}), null, null);
    mv.visitCode();
    new CodeGenSupport(mv).push(model.getRelationID());
    mv.visitInsn(IRETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementGetObjectCodec() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "getObjectCodec", Type
            .getMethodDescriptor(protobufCodec, new Type[] {}), null, null);
    mv.visitCode();
    mv.visitFieldInsn(GETSTATIC, implTypeName, F_OBJECT_CODEC, protobufCodec
        .getDescriptor());
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementGetIndexes() {
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "getIndexes", Type
            .getMethodDescriptor(Type.getType(IndexFunction[].class),
                new Type[] {}), null, null);
    mv.visitCode();
    mv.visitFieldInsn(GETSTATIC, implTypeName, F_INDEXES, Type.getType(
        IndexFunction[].class).getDescriptor());
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementPrimaryKey() {
    final ColumnModel f = key.getField();
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "primaryKey", Type
            .getMethodDescriptor(ormKey, new Type[] {object}), null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 1);
    mv.visitTypeInsn(CHECKCAST, entityType.getInternalName());
    mv.visitFieldInsn(GETFIELD, entityType.getInternalName(), f.getFieldName(),
        CodeGenSupport.toType(f).getDescriptor());
    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementEncodePrimaryKey() throws OrmException {
    final List<ColumnModel> pCols = Collections.singletonList(key.getField());
    final Type argType = CodeGenSupport.toType(key.getField());
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, "encodePrimaryKey", Type
            .getMethodDescriptor(Type.VOID_TYPE, new Type[] {indexKeyBuilder,
                ormKey}), null, null);
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 2);
    mv.visitTypeInsn(CHECKCAST, argType.getInternalName());
    mv.visitVarInsn(ASTORE, 2);

    final QueryCGS cgs =
        new QueryCGS(mv, new Type[] {argType}, pCols, new int[] {2}, 1);
    for (ColumnModel f : pCols) {
      IndexFunctionGen.encodeField(f, mv, cgs);
    }

    mv.visitInsn(RETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementKeyQuery(KeyModel key) {
    final Type keyType = CodeGenSupport.toType(key.getField());
    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, key.getName(), Type
            .getMethodDescriptor(entityType, new Type[] {keyType}), null,
            new String[] {Type.getType(OrmException.class).getInternalName()});
    mv.visitCode();

    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitMethodInsn(INVOKESPECIAL, accessType.getInternalName(), "get", Type
        .getMethodDescriptor(object, new Type[] {ormKey}));
    mv.visitTypeInsn(CHECKCAST, entityType.getInternalName());

    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private void implementQuery(final QueryModel info) throws OrmException {
    final List<ColumnModel> pCols = info.getParameters();
    final boolean hasLimitParam = info.hasLimitParameter();
    final Type[] pTypes = new Type[pCols.size() + (hasLimitParam ? 1 : 0)];
    final int[] pVars = new int[pTypes.length];
    int nextVar = 1;
    for (int i = 0; i < pCols.size(); i++) {
      pTypes[i] = CodeGenSupport.toType(pCols.get(i));
      pVars[i] = nextVar;
      nextVar += pTypes[i].getSize();
    }
    if (hasLimitParam) {
      pTypes[pTypes.length - 1] = Type.INT_TYPE;
      pVars[pTypes.length - 1] = nextVar;
      nextVar += Type.INT_TYPE.getSize();
    }

    final MethodVisitor mv =
        cw.visitMethod(ACC_PUBLIC | ACC_FINAL, info.getName(), Type
            .getMethodDescriptor(resultSet, pTypes), null,
            new String[] {ormException.getInternalName()});
    mv.visitCode();

    final List<Tree> ops = compareOpsOnly(info.getParseTree());

    // Generate fromKey
    //
    final int fromBuf = nextVar++;
    mv.visitTypeInsn(NEW, indexKeyBuilder.getInternalName());
    mv.visitInsn(DUP);
    mv.visitMethodInsn(INVOKESPECIAL, indexKeyBuilder.getInternalName(),
        "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
    mv.visitVarInsn(ASTORE, fromBuf);

    QueryCGS cgs = new QueryCGS(mv, pTypes, pCols, pVars, fromBuf);
    encodeFields(info, ops, mv, cgs, true /* fromKey */);

    // Generate toKey
    //
    final int toBuf = nextVar++;
    mv.visitTypeInsn(NEW, indexKeyBuilder.getInternalName());
    mv.visitInsn(DUP);
    mv.visitMethodInsn(INVOKESPECIAL, indexKeyBuilder.getInternalName(),
        "<init>", Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {}));
    mv.visitVarInsn(ASTORE, toBuf);

    cgs = new QueryCGS(mv, pTypes, pCols, pVars, toBuf);
    encodeFields(info, ops, mv, cgs, false /* fromKey */);
    cgs.infinity();

    // Make the scan call
    //
    mv.visitVarInsn(ALOAD, 0);
    if (needsIndexFunction(info)) {
      mv.visitFieldInsn(GETSTATIC, implTypeName, "index_" + info.getName(),
          indexFunction.getDescriptor());
    }

    mv.visitVarInsn(ALOAD, fromBuf);
    mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
        "toByteArray", Type.getMethodDescriptor(byteArray, new Type[] {}));

    mv.visitVarInsn(ALOAD, toBuf);
    mv.visitMethodInsn(INVOKEVIRTUAL, indexKeyBuilder.getInternalName(),
        "toByteArray", Type.getMethodDescriptor(byteArray, new Type[] {}));

    // Set the limit on the number of results.
    //
    if (info.hasLimit()) {
      if (hasLimitParam) {
        mv.visitVarInsn(ILOAD, pVars[pTypes.length - 1]);
      } else {
        cgs.push(info.getStaticLimit());
      }
    } else {
      cgs.push(0);
    }

    // Only keep order if there is an order by clause present
    //
    cgs.push(info.hasOrderBy() ? 1 : 0);

    if (needsIndexFunction(info)) {
      mv.visitMethodInsn(INVOKEVIRTUAL, accessType.getInternalName(),
          "scanIndex", Type.getMethodDescriptor(resultSet, new Type[] {
              indexFunction, byteArray, byteArray, Type.INT_TYPE,
              Type.BOOLEAN_TYPE}));
    } else {
      // No where and no order by clause? Use the primary key instead.
      //
      mv.visitMethodInsn(INVOKEVIRTUAL, accessType.getInternalName(),
          "scanPrimaryKey", Type.getMethodDescriptor(resultSet, new Type[] {
              byteArray, byteArray, Type.INT_TYPE, Type.BOOLEAN_TYPE}));
    }

    mv.visitInsn(ARETURN);
    mv.visitMaxs(-1, -1);
    mv.visitEnd();
  }

  private boolean needsIndexFunction(final QueryModel info) {
    return info.hasWhere() || info.hasOrderBy();
  }

  private void encodeFields(QueryModel qm, List<Tree> query, MethodVisitor mv,
      QueryCGS cgs, boolean fromKey) throws OrmException {
    final boolean toKey = !fromKey;
    Tree lastNode = null;

    for (Tree node : query) {
      switch (node.getType()) {
        case QueryParser.GE:
          if (fromKey) {
            checkLastNode(qm, lastNode);
            encodeField(node, mv, cgs);
            cgs.delimiter();
            lastNode = node;
          }
          break;

        case QueryParser.GT:
          if (fromKey) {
            checkLastNode(qm, lastNode);
            encodeField(node, mv, cgs);
            cgs.delimiter();
            cgs.infinity();
            lastNode = node;
          }
          break;

        case QueryParser.EQ:
          checkLastNode(qm, lastNode);
          encodeField(node, mv, cgs);
          cgs.delimiter();
          break;

        case QueryParser.LE:
          if (toKey) {
            checkLastNode(qm, lastNode);
            encodeField(node, mv, cgs);
            cgs.delimiter();
            lastNode = node;
          }
          break;

        case QueryParser.LT:
          if (toKey) {
            checkLastNode(qm, lastNode);
            encodeField(node, mv, cgs);
            cgs.delimiter();
            cgs.nul();
            lastNode = node;
          }
          break;

        default:
          throw new OrmException("Unsupported query token in "
              + model.getMethodName() + "." + qm.getName() + ": "
              + node.toStringTree());
      }

      cgs.nextParameter();
    }
  }

  private void checkLastNode(QueryModel qm, Tree lastNode) throws OrmException {
    if (lastNode != null) {
      throw new OrmException(lastNode.getText() + " must be last operator in "
          + model.getMethodName() + "." + qm.getName());
    }
  }

  private void encodeField(Tree node, MethodVisitor mv, QueryCGS cgs)
      throws OrmException {
    ColumnModel f = ((QueryParser.Column) node.getChild(0)).getField();
    IndexFunctionGen.encodeField(f, mv, cgs);
  }

  private List<Tree> compareOpsOnly(Tree node) throws OrmException {
    if (node == null) {
      return Collections.emptyList();
    }

    switch (node.getType()) {
      case 0: // nil node used to join other nodes together
      case QueryParser.WHERE:
      case QueryParser.AND: {
        List<Tree> res = new ArrayList<Tree>();
        for (int i = 0; i < node.getChildCount(); i++) {
          res.addAll(compareOpsOnly(node.getChild(i)));
        }
        return res;
      }

      case QueryParser.GT:
      case QueryParser.GE:
      case QueryParser.EQ:
      case QueryParser.LE:
      case QueryParser.LT: {
        final Tree lhs = node.getChild(0);
        final Tree rhs = node.getChild(1);
        if (lhs.getType() != QueryParser.ID) {
          throw new OrmException("Unsupported query token");
        }
        if (rhs.getType() == QueryParser.PLACEHOLDER) {
          return Collections.singletonList(node);
        }
        break;
      }

      case QueryParser.ORDER:
      case QueryParser.LIMIT:
        break;

      default:
        throw new OrmException("Unsupported query token " + node.toStringTree());
    }
    return Collections.emptyList();
  }

  private final class QueryCGS extends IndexFunctionGen.EncodeCGS {
    private final Type[] pTypes;
    private final List<ColumnModel> pCols;
    private final int[] pVars;
    private final int bufvar;
    private int currentp;

    private QueryCGS(MethodVisitor method, Type[] pTypes,
        List<ColumnModel> pCols, int[] pVars, int bufvar) {
      super(method);
      this.pTypes = pTypes;
      this.pCols = pCols;
      this.pVars = pVars;
      this.bufvar = bufvar;
    }

    void nextParameter() {
      currentp++;
    }

    @Override
    void pushBuilder() {
      mv.visitVarInsn(ALOAD, bufvar);
    }

    @Override
    public void pushFieldValue() {
      appendGetField(getFieldReference());
    }

    @Override
    protected void appendGetField(final ColumnModel c) {
      if (currentp < pTypes.length && pCols.get(currentp).equals(c)) {
        loadVar(pTypes[currentp], pVars[currentp]);
      } else {
        super.appendGetField(c);
      }
    }
  }
}
