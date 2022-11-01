/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
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

package site.ycsb.db;

import com.adobe.dx.aep.aerospike.poc.uis.UISAerospikeGraphReader;
import com.adobe.dx.aep.aerospike.poc.uis.UISAerospikeGraphWriter;
import com.adobe.dx.aep.poc.uis.entities.Graph;
import com.aerospike.client.policy.WritePolicy;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeClient extends site.ycsb.DB {

  private UISAerospikeGraphWriter writer;
  private UISAerospikeGraphReader reader;

  @Override
  public void init() throws DBException {
    writer = new UISAerospikeGraphWriter();
    reader = new UISAerospikeGraphReader();
  }

  @Override
  public void cleanup() throws DBException {

  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {

      Graph graph = reader.readNode(key+"-0");

      if (graph == null || graph.getGraphData() == null) {
        System.err.println("Record key " + key + " not found (read)");
        return Status.ERROR;
      }

      result.put(key,
          new ByteArrayByteIterator(graph.getGraphData().toString().getBytes(StandardCharsets.UTF_8)));

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error while reading key " + key + ": " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, WritePolicy writePolicy,
                       Map<String, ByteIterator> values) {

    try {
      writer.createEdge(key+"-0,"+key+"-1");
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error while writing key " + key + ": " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      writer.createEdge(key+"-0,"+key+"-2");
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error while updating key " + key + ": " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, new WritePolicy(), values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      writer.deleteEdge(key+"-0,"+key+"-1");
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}