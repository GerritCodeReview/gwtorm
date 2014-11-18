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

package com.google.gwtorm.jdbc;

import com.google.gwtorm.client.KeyUtil;
import com.google.gwtorm.schema.RelationModel;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.schema.sql.SqlDialect;
import com.google.gwtorm.server.GeneratedClassLoader;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;
import com.google.gwtorm.server.SchemaConstructorGen;
import com.google.gwtorm.server.SchemaFactory;
import com.google.gwtorm.server.SchemaGen;
import com.google.gwtorm.server.StandardKeyEncoder;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Constructor for application {@link Schema} extensions.
 * <p>
 * Applications should use the Database class to create instances of their
 * Schema extension interface, and thus open and connect to the JDBC data store.
 * <p>
 * Creating a new Database instance is expensive, due to the type analysis and
 * code generation performed to implement the Schema and Access interfaces.
 * Applications should create and cache their Database instance for the live of
 * the application.
 * <p>
 * Database instances are thread-safe, but returned Schema instances are not.
 *
 * @param <T>
 */
public class Database<T extends Schema> implements SchemaFactory<T> {
  static {
    KeyUtil.setEncoderImpl(new StandardKeyEncoder());
  }

  private final DataSource dataSource;
  private final JavaSchemaModel schemaModel;
  private final SchemaFactory<T> implFactory;
  private final SqlDialect implDialect;

  /**
   * Create a new database interface, generating the interface implementations.
   *
   * @param ds JDBC connection information
   * @param schema application extension of the Schema interface to implement.
   * @throws OrmException the schema interface is incorrectly defined, or the
   *         driver class is not available through the current class loader.
   */
  public Database(final DataSource ds, final Class<T> schema)
      throws OrmException {
    dataSource = ds;

    SqlDialect dialect;
    try {
      Connection c = ds.getConnection();
      try {
        dialect = SqlDialect.getDialectFor(c);
      } finally {
        c.close();
      }
    } catch (SQLException e) {
      throw new OrmException("Unable to determine SqlDialect", e);
    }

    schemaModel = new JavaSchemaModel(schema);
    final GeneratedClassLoader loader = newLoader(schema);
    final Class<T> impl = generate(dialect, loader);
    implFactory = new SchemaConstructorGen<>(loader, impl, this).create();
    implDialect = dialect;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Class<T> generate(final SqlDialect dialect,
      final GeneratedClassLoader loader) throws OrmException {
    return new SchemaGen(loader, schemaModel, getClass(), JdbcSchema.class,
        new SchemaGen.AccessGenerator() {
          @Override
          public Class<?> create(GeneratedClassLoader loader, RelationModel rm)
              throws OrmException {
            return new AccessGen(loader, rm, dialect).create();
          }
        }).create();
  }

  SqlDialect getDialect() {
    return implDialect;
  }

  SchemaModel getSchemaModel() {
    return schemaModel;
  }

  /**
   * Open a new connection to the database and get a Schema wrapper.
   *
   * @return a new JDBC connection, wrapped up in the application's Schema.
   * @throws OrmException the connection could not be opened to the database.
   *         The JDBC exception detail should be examined to determine the root
   *         cause of the connection failure.
   */
  @Override
  public T open() throws OrmException {
    return implFactory.open();
  }

  Connection newConnection() throws OrmException {
    final Connection conn;
    try {
      conn = dataSource.getConnection();
    } catch (SQLException e) {
      throw new OrmException("Cannot open database connection", e);
    }

    try {
      if (!conn.getAutoCommit()) {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      try {
        conn.close();
      } catch (SQLException e2) {
      }
      throw new OrmException("Cannot force auto-commit on connection", e);
    }
    return conn;
  }

  private static <T> GeneratedClassLoader newLoader(final Class<T> schema) {
    return new GeneratedClassLoader(schema.getClassLoader());
  }
}
