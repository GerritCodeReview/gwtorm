package com.google.gwtorm.schema.sql;

/** Dialect for <a href="http://www.h2database.com/">H2</a> */
public class DialectH2 extends SqlDialect {
  @Override
  public String getNextSequenceValueSql(final String seqname) {
    return "SELECT NEXT VALUE FOR " + seqname;
  }
}
