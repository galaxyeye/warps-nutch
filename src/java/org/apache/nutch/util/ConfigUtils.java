/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a getConf of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Utility to create Hadoop {@link Configuration}s that include Nutch-specific
 * resources.
 */
public class ConfigUtils {

  public static final String UUID_KEY = "nutch.conf.uuid";

  /**
   * Create a {@link Configuration} for Nutch. This will load the standard Nutch
   * resources, <code>nutch-default.xml</code> and <code>nutch-site.xml</code>
   * overrides.
   */
  public static Configuration create() {
    Configuration conf = new Configuration();

    setUUID(conf);
    addNutchResources(conf);

    return conf;
  }

  /**
   * Get a unsigned integer, if the configured value is negative, return the default value
   *
   * @param name         The property name
   * @param defaultValue The default value return if the configured value is negative
   * @return a positive integer
   */
  public static Integer getUint(Configuration conf, String name, Integer defaultValue) {
    Integer value = conf.getInt(name, defaultValue);
    if (value < 0) {
      value = defaultValue;
    }
    return value;
  }

  /**
   * Get a unsigned long integer, if the configured value is negative, return the default value
   *
   * @param name         The property name
   * @param defaultValue The default value return if the configured value is negative
   * @return a positive long integer
   */
  public static Long getUlong(Configuration conf, String name, Long defaultValue) {
    Long value = conf.getLong(name, defaultValue);
    if (value < 0) {
      value = defaultValue;
    }
    return value;
  }

  public static void setIfNotNull(Configuration conf, String name, String value) {
    if (name != null && value != null) {
      conf.set(name, value);
    }
  }

  public static void setIfNotEmpty(Configuration conf, String name, String value) {
    if (StringUtils.isNoneEmpty(name) && StringUtils.isNoneEmpty(value)) {
      conf.set(name, value);
    }
  }

  public static Duration getDuration(Configuration conf, String name, Duration defaultValue) {
    long value = conf.getTimeDuration(name, Long.MIN_VALUE, TimeUnit.MILLISECONDS);
    if (value < 0) {
      return defaultValue;
    }
    return Duration.ofMillis(value);
  }

  public static Path getPath(Configuration conf, String name, Path defaultValue) throws IOException {
    String value = conf.get(name);
    Path path = (value != null) ? Paths.get(value) : defaultValue;
    Files.createDirectories(path.getParent());
    return path;
  }

  /*
   * Configuration.hashCode() doesn't return values that correspond to a unique
   * set of parameters. This is a workaround so that we can track instances of
   * Configuration created by Nutch.
   */
  private static void setUUID(Configuration conf) {
    UUID uuid = UUID.randomUUID();
    conf.set(UUID_KEY, uuid.toString());
  }

  /**
   * Retrieve a Nutch UUID of this configuration object, or null if the
   * configuration was created elsewhere.
   * 
   * @return uuid or null
   */
  public static String getUUID(Configuration conf) {
    return conf.get(UUID_KEY);
  }

  public void addClassPath(String s) throws Exception {
    Path path = Paths.get(s);
    URL u = path.toUri().toURL();
    URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(urlClassLoader, u);
  }

  /**
   * Add the standard Nutch resources to {@link Configuration}.
   * 
   */
  private static void addNutchResources(Configuration conf) {
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
  }
}
