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

package com.google.gwtorm.nosql.heap;

import com.google.gwtorm.server.OrmException;
import com.google.gwtorm.server.Schema;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * Tiny NoSQL database stored on the local filesystem.
 * <p>
 * This is a simple NoSQL implementation intended only for development/debugging
 * purposes. It is not capable of supporting any production traffic. Large data
 * sets will cause the implementation to fall over, as all records are stored in
 * memory.
 * <p>
 * Although some effort is made to persist data to disk during updates, and
 * reload it during construction, durability of stored data is not guaranteed.
 *
 * @param <T> type of the application schema.
 */
public class FileDatabase<T extends Schema> extends
    TreeMapDatabase<T, FileDatabase.LoggingSchema, FileDatabase.LoggingAccess> {
  private static final int MAX_LOG_SIZE = 50000;

  private final File heapFile;
  private final File logFile;

  private RandomAccessFile log;
  private int logRecords;

  /**
   * Create the database and implement the application's schema interface.
   *
   * @param path path prefix for the data files. File suffixes will be added to
   *        this name to name the database's various files.
   * @param schema the application schema this database will open.
   * @throws OrmException the schema cannot be queried, or the existing database
   *         files are not readable.
   */
  public FileDatabase(final File path, final Class<T> schema)
      throws OrmException {
    super(LoggingSchema.class, LoggingAccess.class, schema);

    heapFile = new File(path.getAbsolutePath() + ".nosql_db");
    logFile = new File(path.getAbsolutePath() + ".nosql_log");

    lock.lock();
    try {
      loadHeap();
      loadLog();
    } catch (IOException err) {
      throw new OrmException("Cannot load existing database", err);
    } finally {
      lock.unlock();
    }
  }

  /** Gracefully close the database and its log file. */
  public void close() throws OrmException {
    lock.lock();
    try {
      if (log != null) {
        try {
          log.close();
        } catch (IOException err) {
          throw new OrmException("Cannot close log file", err);
        } finally {
          log = null;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void loadHeap() throws IOException {
    lock.lock();
    try {
      table.clear();

      final DataInputStream in;
      try {
        in = new DataInputStream( //
            new BufferedInputStream( //
                new FileInputStream(heapFile)));
      } catch (FileNotFoundException e) {
        return;
      }

      try {
        final int cnt = in.readInt();
        for (int row = 0; row < cnt; row++) {
          final byte[] key = new byte[in.readInt()];
          final byte[] val = new byte[in.readInt()];
          in.readFully(key);
          in.readFully(val);
          table.put(key, val);
        }
      } finally {
        in.close();
      }
    } finally {
      lock.unlock();
    }
  }

  private void loadLog() throws IOException, OrmException {
    lock.lock();
    try {
      logRecords = 0;

      final DataInputStream in;
      try {
        in = new DataInputStream( //
            new BufferedInputStream( //
                new FileInputStream(logFile)));
      } catch (FileNotFoundException e) {
        return;
      }

      try {
        for (;; logRecords++) {
          final int op = in.read();
          if (op < 0) {
            break;
          }

          switch (op) {
            case 0: {
              final byte[] key = new byte[in.readInt()];
              in.readFully(key);
              table.remove(key);
              break;
            }

            case 1: {
              final byte[] key = new byte[in.readInt()];
              final byte[] val = new byte[in.readInt()];
              in.readFully(key);
              in.readFully(val);
              table.put(key, val);
              break;
            }

            default:
              throw new OrmException("Unknown log command " + op);
          }
        }
      } finally {
        in.close();
      }
    } finally {
      lock.unlock();
    }
  }

  private void writeLog(int op, byte[] key, byte[] val) throws OrmException {
    if (logRecords == MAX_LOG_SIZE) {
      compact();
      return;
    }

    try {
      openLog();

      int sz = 1 + 4 + key.length;
      if (op == 1) {
        sz += 4 + val.length;
      }

      final ByteArrayOutputStream buf = new ByteArrayOutputStream(sz);
      final DataOutputStream out = new DataOutputStream(buf);

      out.write(op);
      out.writeInt(key.length);
      if (op == 1) {
        out.writeInt(val.length);
      }
      out.write(key);
      if (op == 1) {
        out.write(val);
      }
      out.flush();

      log.write(buf.toByteArray());
      logRecords++;
    } catch (IOException err) {
      throw new OrmException("Cannot log operation", err);
    }
  }

  private void compact() throws OrmException {
    lock.lock();
    try {
      final File tmp = newTempFile();
      boolean ok = false;
      try {
        DataOutputStream out = new DataOutputStream( //
            new BufferedOutputStream( //
                new FileOutputStream(tmp)));
        try {
          out.writeInt(table.size());
          for (Map.Entry<byte[], byte[]> ent : table.entrySet()) {
            out.writeInt(ent.getKey().length);
            out.writeInt(ent.getValue().length);
            out.write(ent.getKey());
            out.write(ent.getValue());
          }
        } finally {
          out.close();
        }

        if (!tmp.renameTo(heapFile)) {
          throw new OrmException("Cannot replace " + heapFile);
        }

        ok = true;

        openLog();
        log.seek(0);
        log.setLength(0);

      } finally {
        if (!ok) {
          if (!tmp.delete()) {
            tmp.deleteOnExit();
          }
        }
      }
    } catch (IOException err) {
      throw new OrmException("Cannot compact database", err);
    } finally {
      lock.unlock();
    }
  }

  private void openLog() throws IOException {
    if (log == null) {
      log = new RandomAccessFile(logFile, "rws");
      log.seek(log.length());
    }
  }

  private File newTempFile() throws IOException {
    return File.createTempFile("heap_", "_db", heapFile.getParentFile());
  }

  public static abstract class LoggingSchema extends TreeMapSchema {
    private final FileDatabase<?> db;

    protected LoggingSchema(FileDatabase<?> db) {
      super(db);
      this.db = db;
    }

    @Override
    public void upsert(byte[] key, byte[] data) throws OrmException {
      db.lock.lock();
      try {
        super.upsert(key, data);
        db.writeLog(1, key, data);
      } finally {
        db.lock.unlock();
      }
    }

    @Override
    public void delete(byte[] key) throws OrmException {
      db.lock.lock();
      try {
        super.delete(key);
        db.writeLog(0, key, null);
      } finally {
        db.lock.unlock();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static abstract class LoggingAccess extends TreeMapAccess {
    protected LoggingAccess(LoggingSchema s) {
      super(s);
    }
  }
}
