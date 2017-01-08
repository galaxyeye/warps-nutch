/**
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
 */
package org.apache.nutch.common;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.WeakHashMap;

public class ObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class);

  private static final WeakHashMap<Configuration, ObjectCache> CACHE = new WeakHashMap<>();

  private final HashMap<String, Object> objectMap = new HashMap<>();

  private ObjectCache() {}

  public static ObjectCache get(Configuration conf) {
    ObjectCache objectCache = CACHE.get(conf);
    if (objectCache == null) {
      objectCache = new ObjectCache();
      CACHE.put(conf, objectCache);
    }

    return objectCache;
  }

  public Object getObject(String key) { return objectMap.get(key); }

  public <T> T getObject(String key, T defaultValue) {
    Object obj = objectMap.get(key);
    if (obj == null) {
      return defaultValue;
    }
    return (T)obj;
  }

  public void setObject(String key, Object value) {
    objectMap.put(key, value);
  }
}
