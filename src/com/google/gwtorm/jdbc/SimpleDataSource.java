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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

/** A simple non-pooling DataSource representation. */
public class SimpleDataSource implements DataSource {
  private final Properties connectionInfo;
  private final String url;
  private PrintWriter logWriter;

  /**
   * Create a non-pooling data source.
   * <p>
   * The JDBC properties information must define at least <code>url</code> and
   * <code>driver</code>, but may also include driver specific properties such
   * as <code>username</code> and <code>password</code>.
   * 
   * @param dbInfo JDBC connection information. The property table is copied.
   * @throws SQLException the driver class is not available through the current
   *         class loader.
   */
  public SimpleDataSource(final Properties dbInfo) throws SQLException {
    connectionInfo = new Properties();
    connectionInfo.putAll(dbInfo);

    url = (String) connectionInfo.remove("url");
    if (url == null) {
      throw new SQLException("Required property 'url' not defined");
    }

    final String driver = (String) connectionInfo.remove("driver");
    if (driver != null) {
      loadDriver(driver);
    }

    logWriter = new PrintWriter(System.out);
  }

  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(url, connectionInfo);
  }

  public Connection getConnection(String username, String password)
      throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }

  public PrintWriter getLogWriter() {
    return logWriter;
  }

  public void setLogWriter(final PrintWriter out) {
    logWriter = out;
  }

  public int getLoginTimeout() {
    return 0;
  }

  public void setLoginTimeout(int seconds) {
  }

  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(getClass().getName() + " wraps nothing");
  }

  private static synchronized void loadDriver(final String driver)
      throws SQLException {
    // I've seen some drivers (*cough* Informix *cough*) which won't load
    // on multiple threads at the same time. Forcing our code to synchronize
    // around loading the driver ensures we won't ever ask for the same driver
    // to initialize from different threads. Of course that could still happen
    // in other parts of the same JVM, but its quite unlikely.
    //
    try {
      Class.forName(driver, true, threadCL());
    } catch (ClassNotFoundException err) {
      final SQLException e;
      e = new SQLException("Driver class " + driver + " not available");
      e.initCause(err);
      throw e;
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
