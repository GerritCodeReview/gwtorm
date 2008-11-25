package com.google.gwtorm.schema.sql;

import java.sql.Types;

/** Dialect for <a href="http://www.postgresql.org/>PostgreSQL</a> */
public class DialectPostgreSQL extends SqlDialect {
  public DialectPostgreSQL() {
    typeNames.put(Types.VARBINARY, "BYTEA");
  }

  @Override
  public String getNextSequenceValueSql(final String seqname) {
    return "SELECT nextval('" + seqname + "')";
  }
}
