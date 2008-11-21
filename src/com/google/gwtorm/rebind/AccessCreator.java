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

package com.google.gwtorm.rebind;

import com.google.gwt.core.ext.GeneratorContext;
import com.google.gwt.core.ext.TreeLogger;
import com.google.gwt.core.ext.UnableToCompleteException;
import com.google.gwt.core.ext.typeinfo.JArrayType;
import com.google.gwt.core.ext.typeinfo.JClassType;
import com.google.gwt.core.ext.typeinfo.JPackage;
import com.google.gwt.core.ext.typeinfo.JType;
import com.google.gwt.core.ext.typeinfo.TypeOracle;
import com.google.gwt.user.rebind.ClassSourceFileComposerFactory;
import com.google.gwt.user.rebind.SourceWriter;

import java.io.PrintWriter;

class AccessCreator {
  private static final String ACCESS_SUFFIX = "_DataAccess";
  private JClassType svcInf;

  AccessCreator(final JClassType remoteService) {
    svcInf = remoteService;
  }

  String create(final TreeLogger logger, final GeneratorContext context)
      throws UnableToCompleteException {
    final TypeOracle typeOracle = context.getTypeOracle();

    final SourceWriter srcWriter = getSourceWriter(logger, context);
    if (srcWriter == null) {
      return getProxyQualifiedName();
    }

    srcWriter.commit(logger);

    return getProxyQualifiedName();
  }

  private SourceWriter getSourceWriter(final TreeLogger logger,
      final GeneratorContext ctx) {
    final JPackage servicePkg = svcInf.getPackage();
    final String pkgName = servicePkg == null ? "" : servicePkg.getName();
    final PrintWriter pw;
    final ClassSourceFileComposerFactory cf;

    pw = ctx.tryCreate(logger, pkgName, getProxySimpleName());
    if (pw == null) {
      return null;
    }

    cf = new ClassSourceFileComposerFactory(pkgName, getProxySimpleName());
    cf.addImplementedInterface(svcInf.getErasedType().getQualifiedSourceName());
    return cf.createSourceWriter(ctx, pw);
  }

  private String getProxyQualifiedName() {
    final String[] name = synthesizeTopLevelClassName(svcInf, ACCESS_SUFFIX);
    return name[0].length() == 0 ? name[1] : name[0] + "." + name[1];
  }

  private String getProxySimpleName() {
    return synthesizeTopLevelClassName(svcInf, ACCESS_SUFFIX)[1];
  }

  static String[] synthesizeTopLevelClassName(JClassType type, String suffix) {
    // Gets the basic name of the type. If it's a nested type, the type name
    // will contains dots.
    //
    String className;
    String packageName;

    JType leafType = type.getLeafType();
    if (leafType.isPrimitive() != null) {
      className = leafType.getSimpleSourceName();
      packageName = "";
    } else {
      JClassType classOrInterface = leafType.isClassOrInterface();
      assert (classOrInterface != null);
      className = classOrInterface.getName();
      packageName = classOrInterface.getPackage().getName();
    }

    JArrayType isArray = type.isArray();
    if (isArray != null) {
      className += "_Array_Rank_" + isArray.getRank();
    }

    // Add the meaningful suffix.
    //
    className += suffix;

    // Make it a top-level name.
    //
    className = className.replace('.', '_');

    return new String[] {packageName, className};
  }
}
