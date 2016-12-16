/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.storage;

import org.apache.gora.filter.Filter;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.storage.gora.Host;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Entry point to Gora store/mapreduce functionality. Translates the concept of
 * "crawlid" to the corresponding Gora support.
 */
public class StorageUtils {

  /**
   * Creates a store for the given persistentClass. Currently supports Webpage
   * and Host stores.
   * 
   * @param conf
   * @param keyClass
   * @param persistentClass
   * @return
   * @throws ClassNotFoundException
   * @throws GoraException
   */
  @SuppressWarnings("unchecked")
  public static <K, V extends Persistent> DataStore<K, V> createWebStore(
      Configuration conf, Class<K> keyClass, Class<V> persistentClass)
      throws ClassNotFoundException, GoraException {

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID, "");
    String schemaPrefix = "";
    if (!crawlId.isEmpty()) {
      schemaPrefix = crawlId + "_";
    }

    String schema;
    if (GoraWebPage.class.equals(persistentClass)) {
      schema = conf.get("storage.schema.webpage", "webpage");
      conf.set("preferred.schema.name", schemaPrefix + "webpage");
    } else if (Host.class.equals(persistentClass)) {
      schema = conf.get("storage.schema.host", "host");
      conf.set("preferred.schema.name", schemaPrefix + "host");
    } else {
      throw new UnsupportedOperationException(
          "Unable to create store for class " + persistentClass);
    }

    Class<? extends DataStore<K, V>> dataStoreClass = getDataStoreClass(conf);
    return DataStoreFactory.createDataStore(dataStoreClass, keyClass, persistentClass, conf, schema);
  }

  /**
   * Return the Persistent Gora class used to persist Nutch Web data.
   * 
   * @param conf
   *          Nutch configuration
   * @return the Gora DataStore persistent class
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static <K, V extends Persistent> Class<? extends DataStore<K, V>> getDataStoreClass(
      Configuration conf) throws ClassNotFoundException {
    return (Class<? extends DataStore<K, V>>) Class.forName(conf.get(
        "storage.data.store.class", "org.apache.gora.sql.store.SqlStore"));
  }

  public static <K, V> void initMapperJob(Job job,
                                          Collection<GoraWebPage.Field> fields, Class<K> outKeyClass,
                                          Class<V> outValueClass,
                                          Class<? extends GoraMapper<String, GoraWebPage, K, V>> mapperClass)
      throws ClassNotFoundException, IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass, mapperClass, null,
        true);
  }

  public static <K, V> void initMapperJob(Job job,
                                          Collection<GoraWebPage.Field> fields, Class<K> outKeyClass,
                                          Class<V> outValueClass,
                                          Class<? extends GoraMapper<String, GoraWebPage, K, V>> mapperClass,
                                          Class<? extends Partitioner<K, V>> partitionerClass)
      throws ClassNotFoundException, IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass, mapperClass,
        partitionerClass, true);
  }

  public static <K, V> void initMapperJob(Job job,
                                          Collection<GoraWebPage.Field> fields, Class<K> outKeyClass,
                                          Class<V> outValueClass,
                                          Class<? extends GoraMapper<String, GoraWebPage, K, V>> mapperClass,
                                          Class<? extends Partitioner<K, V>> partitionerClass, boolean reuseObjects)
      throws ClassNotFoundException, IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass, mapperClass,
        partitionerClass, null, reuseObjects);
  }

  public static <K, V> void initMapperJob(Job job,
                                          Collection<GoraWebPage.Field> fields, Class<K> outKeyClass,
                                          Class<V> outValueClass,
                                          Class<? extends GoraMapper<String, GoraWebPage, K, V>> mapperClass,
                                          Class<? extends Partitioner<K, V>> partitionerClass,
                                          Filter<String, GoraWebPage> filter, boolean reuseObjects)
      throws ClassNotFoundException, IOException {
    DataStore<String, GoraWebPage> store = createWebStore(job.getConfiguration(), String.class, GoraWebPage.class);
    if (store == null) {
      throw new RuntimeException("Could not create datastore");
    }

    Query<String, GoraWebPage> query = store.newQuery();
    query.setFields(toStringArray(fields));
    if (filter != null) {
      query.setFilter(filter);
    }

    GoraMapper.initMapperJob(job, query, store, outKeyClass, outValueClass, mapperClass, partitionerClass, reuseObjects);
    GoraOutputFormat.setOutput(job, store, true);
  }

  public static <K, V> void initMapperJob(Job job,
                                          Collection<GoraWebPage.Field> fields, Class<K> outKeyClass,
                                          Class<V> outValueClass,
                                          Class<? extends GoraMapper<String, GoraWebPage, K, V>> mapperClass,
                                          Filter<String, GoraWebPage> filter) throws ClassNotFoundException,
      IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass, mapperClass, null,
        filter, true);
  }

  public static <K, V> void initReducerJob(Job job,
      Class<? extends GoraReducer<K, V, String, GoraWebPage>> reducerClass)
      throws ClassNotFoundException, GoraException {
    Configuration conf = job.getConfiguration();
    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    GoraReducer.initReducerJob(job, store, reducerClass);
    GoraOutputFormat.setOutput(job, store, true);
  }

  public static String[] toStringArray(Collection<GoraWebPage.Field> fields) {
    String[] arr = new String[fields.size()];
    Iterator<GoraWebPage.Field> iter = fields.iterator();
    for (int i = 0; i < arr.length; i++) {
      arr[i] = iter.next().getName();
    }
    return arr;
  }
}
