/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.foundationdb;

import com.adobe.dx.aep.fdb.poc.uis.UISFDBGraphReader;
import com.adobe.dx.aep.fdb.poc.uis.UISFDBGraphWriter;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.Tuple;
import com.adobe.dx.aep.poc.uis.entities.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import java.util.*;

/**
 * FoundationDB client for YCSB framework.
 */

public class FoundationDBClient extends DB {
  private FDB fdb;
  private Database db;
  private String dbName;
  private int batchSize;
  private int batchCount;
  private static final String API_VERSION          = "foundationdb.apiversion";
  private static final String API_VERSION_DEFAULT  = "520";
  private static final String CLUSTER_FILE         = "foundationdb.clusterfile";
  private static final String CLUSTER_FILE_DEFAULT = "./fdb.cluster";
  private static final String DB_NAME              = "foundationdb.dbname";
  private static final String DB_NAME_DEFAULT      = "DB";
  private static final String DB_BATCH_SIZE_DEFAULT = "0";
  private static final String DB_BATCH_SIZE         = "foundationdb.batchsize";

  private Vector<String> batchKeys;
  private Vector<Map<String, ByteIterator>> batchValues;

  private UISFDBGraphReader reader;
  private UISFDBGraphWriter writer;

  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    final Properties props = getProperties();
    String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
    String clusterFile = props.getProperty(CLUSTER_FILE, CLUSTER_FILE_DEFAULT);
    String dbBatchSize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    dbName = props.getProperty(DB_NAME, DB_NAME_DEFAULT);

    logger.info("API Version: {}", apiVersion);
    logger.info("Cluster File: {}\n", clusterFile);

    try {
      reader = new UISFDBGraphReader(apiVersion, clusterFile);
      writer = new UISFDBGraphWriter(apiVersion, clusterFile);
      fdb = reader.getFdb();
      db = reader.getDb();
      batchSize = Integer.parseInt(dbBatchSize);
      batchCount = 0;
      batchKeys = new Vector<String>(batchSize+1);
      batchValues = new Vector<Map<String, ByteIterator>>(batchSize+1);
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      logger.error(MessageFormatter.format("Invalid value for apiversion property: {}", apiVersion).getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {

  }

  private static String getRowKey(String db, String table, String key) {
    //return key + ":" + table + ":" + db;
    return db + ":" + table + ":" + key;
  }

  private static String getEndRowKey(String table) {
    return table + ";";
  }

  private Status convTupleToMap(Tuple tuple, Set<String> fields, Map<String, ByteIterator> result) {
    for (int i = 0; i < tuple.size(); i++) {
      Tuple v = tuple.getNestedTuple(i);
      String field = v.getString(0);
      String value = v.getString(1);
      //System.err.println(field + " : " + value);
      result.put(field, new StringByteIterator(value));
    }
    if (fields != null) {
      for (String field : fields) {
        if (result.get(field) == null) {
          logger.debug("field not fount: {}", field);
          return Status.NOT_FOUND;
        }
      }
    }
    return Status.OK;
  }

  private void batchInsert() {
    try {
      db.run(tr -> {
        for (int i = 0; i < batchCount; ++i) {
          Tuple t = new Tuple();
          for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(batchValues.get(i)).entrySet()) {
            Tuple v = new Tuple();
            v = v.add(entry.getKey());
            v = v.add(entry.getValue());
            t = t.add(v);
          }
          tr.set(Tuple.from(batchKeys.get(i)).pack(), t.pack());
        }
        return null;
      });
    } catch (FDBException e) {
      for (int i = 0; i < batchCount; ++i) {
        logger.error(MessageFormatter.format("Error batch inserting key {}", batchKeys.get(i)).getMessage(), e);
      }
      e.printStackTrace();
    } catch (Throwable e) {
      for (int i = 0; i < batchCount; ++i) {
        logger.error(MessageFormatter.format("Error batch inserting key {}", batchKeys.get(i)).getMessage(), e);
      }
      e.printStackTrace();
    } finally {
      batchKeys.clear();
      batchValues.clear();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    logger.debug("insert key = {}", key);
    try {
      writer.createEdge(key+"-0," + key+"-1");
      return Status.OK;
    } catch (Throwable e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    logger.debug("delete key = {}", key);
    try {
      writer.deleteEdge(key+"-0," + key+"-1");
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    logger.debug("read key = {}", key);
    try {
      Graph graph = reader.readNode(key+"-0");
      return graph != null?Status.OK:Status.NOT_FOUND;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    logger.debug("update key = {}", key);
    try {
        writer.createEdge(key+"-0," + key+"-2");
        return Status.OK;
    } catch (FDBException e) {
        logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
        e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String startRowKey = getRowKey(dbName, table, startkey);
    String endRowKey = getEndRowKey(table);
    logger.debug("scan key from {} to {} limit {} ", startkey, endRowKey, recordcount);
    try (Transaction tr = db.createTransaction()) {
      tr.options().setReadYourWritesDisable();
      AsyncIterable<KeyValue> entryList = tr.getRange(Tuple.from(startRowKey).pack(), Tuple.from(endRowKey).pack(),
          recordcount > 0 ? recordcount : 0);
      List<KeyValue> entries = entryList.asList().join();
      for (int i = 0; i < entries.size(); ++i) {
        final HashMap<String, ByteIterator> map = new HashMap<>();
        Tuple value = Tuple.fromBytes(entries.get(i).getValue());
        if (convTupleToMap(value, fields, map) == Status.OK) {
          result.add(map);
        } else {
          logger.error("Error scanning keys: from {} to {} limit {} ", startRowKey, endRowKey, recordcount);
          return Status.ERROR;
        }
      }
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}