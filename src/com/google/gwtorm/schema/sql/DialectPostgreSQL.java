package com.google.gwtorm.schema.sql;

/** Dialect for <a href="http://www.postgresql.org/>PostgreSQL</a> */
public class DialectPostgreSQL extends SqlDialect {
  @Override
  public String getNextSequenceValueSql(final String seqname) {
    return "SELECT nextval('" + seqname + "')";
  }
}
