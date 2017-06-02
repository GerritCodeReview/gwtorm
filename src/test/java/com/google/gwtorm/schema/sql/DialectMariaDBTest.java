// Copyright 2017 Android Open Source Project.
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

package com.google.gwtorm.schema.sql;

import static org.junit.Assume.assumeNoException;

import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.PhoneBookDb2;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.jdbc.SimpleDataSource;
import java.sql.DriverManager;
import java.util.Properties;
import org.junit.Before;

public class DialectMariaDBTest extends DialectMySQLTest {
  @Override
  @Before
  public void setUp() throws Exception {
    Class.forName(org.mariadb.jdbc.Driver.class.getName());

    final String host = "localhost";
    final String database = "gwtorm";
    final String user = "gwtorm";
    final String pass = "gwtorm";

    final String url = "jdbc:mariadb://" + host + "/" + database;
    try {
      db = DriverManager.getConnection(url, user, pass);
    } catch (Throwable t) {
      assumeNoException(t);
    }
    executor = new JdbcExecutor(db);
    dialect = new DialectMariaDB().refine(db);

    final Properties p = new Properties();
    p.setProperty("driver", org.mariadb.jdbc.Driver.class.getName());
    p.setProperty("url", db.getMetaData().getURL());
    p.setProperty("user", user);
    p.setProperty("password", pass);
    phoneBook = new Database<>(new SimpleDataSource(p), PhoneBookDb.class);
    phoneBook2 = new Database<>(new SimpleDataSource(p), PhoneBookDb2.class);
  }
}
