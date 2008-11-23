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

import com.google.gwtorm.client.OrmException;
import com.google.gwtorm.client.Schema;
import com.google.gwtorm.client.SchemaFactory;
import com.google.gwtorm.jdbc.gen.GeneratedClassLoader;
import com.google.gwtorm.jdbc.gen.SchemaFactoryGen;
import com.google.gwtorm.jdbc.gen.SchemaGen;
import com.google.gwtorm.schema.SchemaModel;
import com.google.gwtorm.schema.java.JavaSchemaModel;
import com.google.gwtorm.schema.sql.DialectH2;
import com.google.gwtorm.schema.sql.DialectPostgreSQL;
import com.google.gwtorm.schema.sql.SqlDialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

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
  private static final Map<Class<?>, String> schemaFactoryNames =
      Collections.synchronizedMap(new WeakHashMap<Class<?>, String>());

  private final Properties connectionInfo;
  private final String url;
  private final JavaSchemaModel schemaModel;
  private final AbstractSchemaFactory<T> implFactory;
  private final SqlDialect implDialect;

  /**
   * Create a new database interface, generating the interface implementations.
   * <p>
   * The JDBC properties information must define at least <code>url</code> and
   * <code>driver</code>, but may also include driver specific properties such
   * as <code>username</code> and <code>password</code>.
   * 
   * @param dbInfo JDBC connection information. The property table is copied.
   * @param schema application extension of the Schema interface to implement.
   * @throws OrmException the schema interface is incorrectly defined, or the
   *         driver class is not available through the current class loader.
   */
  public Database(final Properties dbInfo, final Class<T> schema)
      throws OrmException {
    connectionInfo = new Properties();
    connectionInfo.putAll(dbInfo);

    url = (String) connectionInfo.remove("url");
    if (url == null) {
      throw new OrmException("Required property 'url' not defined");
    }

    final String driver = (String) connectionInfo.remove("driver");
    if (driver != null) {
      loadDriver(driver);
    }

    final SqlDialect dialect;
    String dialectName = (String) connectionInfo.remove("dialect");
    if (dialectName != null) {
      if (!dialectName.contains(".")) {
        final String n = SqlDialect.class.getName();
        dialectName = n.substring(0, n.lastIndexOf('.') + 1) + dialectName;
      }
      try {
        dialect = (SqlDialect) Class.forName(dialectName).newInstance();
      } catch (InstantiationException e) {
        throw new OrmException("Dialect " + dialectName + " not available", e);
      } catch (IllegalAccessException e) {
        throw new OrmException("Dialect " + dialectName + " not available", e);
      } catch (ClassNotFoundException e) {
        throw new OrmException("Dialect " + dialectName + " not found", e);
      }
    } else if (url.startsWith("jdbc:postgresql:")) {
      dialect = new DialectPostgreSQL();
    } else if (url.startsWith("jdbc:h2:")) {
      dialect = new DialectH2();
    } else {
      throw new OrmException("No dialect known for " + url);
    }

    schemaModel = new JavaSchemaModel(schema);
    final GeneratedClassLoader loader = newLoader(schema);
    final String cachedName = schemaFactoryNames.get(schema);
    AbstractSchemaFactory<T> factory = null;
    if (cachedName != null) {
      factory = newFactory(loader, cachedName);
    }
    if (factory == null) {
      final SchemaGen gen = new SchemaGen(loader, schemaModel, dialect);
      gen.defineClass();
      factory = new SchemaFactoryGen<T>(loader, gen).create();
      schemaFactoryNames.put(schema, factory.getClass().getName());
    }
    implFactory = factory;
    implDialect = dialect;
  }

  @SuppressWarnings("unchecked")
  private AbstractSchemaFactory<T> newFactory(final ClassLoader cl,
      final String name) {
    try {
      final Class<?> ft = Class.forName(name, true, cl);
      return (AbstractSchemaFactory<T>) ft.newInstance();
    } catch (InstantiationException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    } catch (ClassNotFoundException e) {
      return null;
    }
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
  public T open() throws OrmException {
    final Connection conn;
    try {
      conn = DriverManager.getConnection(url, connectionInfo);
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

    return implFactory.create(this, conn);
  }

  private static <T> GeneratedClassLoader newLoader(final Class<T> schema) {
    return new GeneratedClassLoader(schema.getClassLoader());
  }

  private static synchronized void loadDriver(final String driver)
      throws OrmException {
    // I've seen some drivers (*cough* Informix *cough*) which won't load
    // on multiple threads at the same time. Forcing our code to synchronize
    // around loading the driver ensures we won't ever ask for the same driver
    // to initialize from different threads. Of course that could still happen
    // in other parts of the same JVM, but its quite unlikely.
    //
    try {
      Class.forName(driver, true, threadCL());
    } catch (ClassNotFoundException err) {
      throw new OrmException("Driver class " + driver + " not available", err);
    }
  }

  private static ClassLoader threadCL() {
    try {
      return Thread.currentThread().getContextClassLoader();
    } catch (SecurityException e) {
      return Database.class.getClassLoader();
    }
  }
}
