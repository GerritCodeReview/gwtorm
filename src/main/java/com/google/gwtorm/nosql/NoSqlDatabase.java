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

import com.google.gwtorm.client.KeyUtil;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.server.GeneratedClassLoader;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;
import com.google.gwtorm.server.SchemaConstructorGen;
import com.google.gwtorm.server.SchemaFactory;
import com.google.gwtorm.server.SchemaGen;
import com.google.gwtorm.server.StandardKeyEncoder;

/**
 * Base class for NoSQL typed databases.
 * <p>
 * Applications should use the database class to create instances of their
 * Schema extension interface, and thus open and connect to the data store.
 * <p>
 * Creating a new database instance is expensive, due to the type analysis and
 * code generation performed to implement the Schema and Access interfaces.
 * Applications should create and cache their database instance for the life of
 * the application.
 * <p>
 * Database instances are thread-safe, but returned Schema instances are not.
 * <p>
 * This class must be further extended by the NoSQL implementation to configure
 * the connectivity with the data store and supply the correct subclass of
 * {@link NoSqlSchema} that knows how to interact with the data store.
 *
 * @param <T> type of the application's Schema.
 * @param <S> type of the implementation's base for Schema implementations.
 * @param <A> type of the implementation's base for Access implementations.
 */
public abstract class NoSqlDatabase<T extends Schema, S extends NoSqlSchema, A extends NoSqlAccess>
    implements SchemaFactory<T> {
  static {
    KeyUtil.setEncoderImpl(new StandardKeyEncoder());
  }

  private final SchemaModel schemaModel;
  private final SchemaFactory<T> implFactory;

  /**
   * Initialize a new database and generate the implementation.
   *
   * @param schemaBaseType class that the generated Schema implementation should
   *        extend in order to provide data store connectivity.
   * @param accessBaseType class that the generated Access implementations
   *        should extend in order to provide single-relation access for each
   *        schema instance.
   * @param appSchema the application schema interface that must be implemented
   *        and constructed on demand.
   * @throws OrmException the schema cannot be created because of an annotation
   *         error in the interface definitions.
   */
  protected NoSqlDatabase(final Class<S> schemaBaseType,
      final Class<A> accessBaseType, final Class<T> appSchema)
      throws OrmException {
    schemaModel = new JavaSchemaModel(appSchema);
    final GeneratedClassLoader loader = newLoader(appSchema);
    final Class<T> impl = generate(schemaBaseType, accessBaseType, loader);
    implFactory = new SchemaConstructorGen<T>(loader, impl, this).create();
  }

  @Override
  public T open() throws OrmException {
    return implFactory.open();
  }

  /** @return the derived model of the application's schema. */
  public SchemaModel getSchemaModel() {
    return schemaModel;
  }

  @SuppressWarnings("unchecked")
  private Class<T> generate(final Class<S> schemaBaseType,
      final Class<A> accessBaseType, final GeneratedClassLoader loader)
      throws OrmException {
    return new SchemaGen(loader, schemaModel, getClass(), schemaBaseType,
        new SchemaGen.AccessGenerator() {
          @Override
          public Class<?> create(GeneratedClassLoader loader, RelationModel rm)
              throws OrmException {
            return new AccessGen(loader, rm, schemaBaseType, accessBaseType)
                .create();
          }
        }).create();
  }

  private static <T> GeneratedClassLoader newLoader(final Class<T> schema) {
    return new GeneratedClassLoader(schema.getClassLoader());
  }
}
