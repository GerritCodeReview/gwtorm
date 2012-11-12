package com.google.gwtorm.schema.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gwtorm.data.Person;
import com.google.gwtorm.data.PhoneBookDb;
import com.google.gwtorm.data.PhoneBookDb2;
import com.google.gwtorm.jdbc.Database;
import com.google.gwtorm.jdbc.JdbcExecutor;
import com.google.gwtorm.server.OrmDuplicateKeyException;
import com.google.gwtorm.server.OrmException;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;



abstract public class SqlDialectTest {

  protected JdbcExecutor executor;
  protected Connection db;
  protected SqlDialect dialect;
  protected Database<PhoneBookDb> phoneBook;
  protected Database<PhoneBookDb2> phoneBook2;


  @Test
  public void itThrowsORMDuplicteKeyExceptionWhenTryingToInsertDuplicates()
      throws Exception {
        PhoneBookDb p = phoneBook.open();
        try {
          p.updateSchema(executor);

          final Person.Key pk = new Person.Key("Bob");
          final Person bob = new Person(pk, 18);
          p.people().insert(Collections.singleton(bob));

          p.people().insert(Collections.singleton(bob));
          fail("Expected " + OrmDuplicateKeyException.class);
        } catch (OrmDuplicateKeyException e) {
          assertTrue(e.getCause() instanceof SQLException);
          assertContainsString(e.getMessage(), p.people().getRelationName());
        } finally {
          p.close();
        }
      }

  @Test
  public void itThrowsAOrmExceptionForOtherErrors() throws Exception {
    PhoneBookDb p = phoneBook.open();
    try {
      p.updateSchema(executor);

      String invalidKey = null;
      final Person.Key pk = new Person.Key(invalidKey);
      final Person bob = new Person(pk, 18);

      p.people().insert(Collections.singleton(bob));
      fail("Expected " + OrmException.class);
    } catch (OrmException e) {
      assertTrue(e.getCause() instanceof SQLException);
      assertContainsString(e.getMessage(), p.people().getRelationName());
    } finally {
      p.close();
    }
  }

  private void assertContainsString(String string, String substring) {
    assertNotNull(string);
    assertTrue(string.contains(substring));
  }

}
