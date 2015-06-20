package com.google.gwtorm.schema.sql;

import com.google.gwtorm.schema.ColumnModel;
import com.google.gwtorm.server.OrmDuplicateKeyException;
import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.StatementExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/** Dialect for <a href="https://db.apache.org/derby//">H2</a> */
public class DialectDerby extends SqlDialect {

  public DialectDerby() {
    typeNames.put(Types.BIGINT, "BIGINT");
    typeNames.put(Types.LONGVARCHAR, "LONG VARCHAR");

    /* model doesn't seem to have a length... :-(
     * Compare SqlByteArrayType and SqlStrinTypeInfo -> getSqlType()
     */
    typeNames.put(Types.VARBINARY, "VARCHAR (32672) FOR BIT DATA");
  }

  @Override
  public boolean handles(String url, Connection c) {
    return url.startsWith("jdbc:derby:");
  }

  @Override
  public Set<String> listSequences(Connection db) throws SQLException {
    Statement s = db.createStatement();
    try {
      ResultSet rs = s.executeQuery("select sequencename from sys.syssequences");
      try {
        Set<String> sequences = new HashSet<>();
        while (rs.next()) {
          sequences.add(rs.getString(1).toLowerCase());
        }
        return sequences;
      } finally {
        rs.close();
      }
    } finally {
      s.close();
    }
  }

  @Override
  public void renameColumn(StatementExecutor e, String tableName,
      String fromColumn, ColumnModel col) throws OrmException {

    StringBuffer sb = new StringBuffer();
    sb.append("RENAME COLUMN ");
    sb.append(tableName);
    sb.append('.');
    sb.append(fromColumn);
    sb.append(" TO ");
    sb.append(col.getColumnName());
    e.execute(sb.toString());
  }

  @Override
  public void renameTable(StatementExecutor e, String from, String to)
      throws OrmException {
    final StringBuilder r = new StringBuilder();
    r.append("RENAME TABLE ");
    r.append(from);
    r.append(" TO ");
    r.append(to);
    r.append(" ");
    e.execute(r.toString());
  }

  @Override
  protected String getNextSequenceValueSql(String seqname) {
    return "VALUES (NEXT VALUE FOR " + seqname + ")";
  }

  @Override
  public String getDropSequenceSql(String name) {
    return "DROP SEQUENCE " + name + " RESTRICT";
  }

  @Override
  public OrmException convertError(String op, String entity, SQLException err) {
    int state = getSQLStateInt(err);
    switch (state) {
      case 23505: // DUPLICATE_KEY_1
        return new OrmDuplicateKeyException(entity, err);

      default:
        return super.convertError(op, entity, err);
    }
  }


}
