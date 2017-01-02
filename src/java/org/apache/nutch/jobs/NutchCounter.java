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
package org.apache.nutch.jobs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.nutch.filter.CrawlFilters;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO : use metrics module, for example, http://metrics.dropwizard.io
 * */
public class NutchCounter {

  public Logger LOG;

  // Default Counters
  public enum Counter {
    rows,

    errors,
    scoringErrors,

    totalPages,
    indexPages,
    detailPages,

    inlinks,
    outlinks,

    tooDeepPages,

    stFetched,
    stRedirTemp,
    stRedirPerm,
    stNotModified,
    stRetry,
    stUnfetched,
    stGone
  }

  private static AtomicInteger counterSequence = new AtomicInteger(0);

  @SuppressWarnings("rawtypes")
  protected final TaskInputOutputContext context;

  protected final Configuration conf;

  private final int id;

  private final String name;

  private final CrawlFilters crawlFilters;

  private String hostname;

  private final String counterGroup = Nutch.STAT_RUNTIME_STATUS;

  // Thread safe for read
  private AtomicInteger countersCount = new AtomicInteger(0);
  // Thread safe for read/write at index
  private ArrayList<String> counterNames = Lists.newArrayList();
  // Thread safe for read/write at index
  private ArrayList<AtomicInteger> globalCounters = Lists.newArrayList();
  // Thread safe for read/write at index
  private ArrayList<AtomicInteger> nativeCounters = Lists.newArrayList();
  // Not thread safe
  private Map<String, Integer> counterIndexes = Maps.newHashMap();

  @SuppressWarnings("rawtypes")
  public NutchCounter(TaskInputOutputContext context) {
    this.context = context;
    this.conf = context.getConfiguration();
    this.id = counterSequence.incrementAndGet();
    this.name = "NutchCounter" + "-" + id;

    String jobName = context.getJobName();
    jobName = StringUtils.substringBeforeLast(jobName, "-");
    jobName = jobName.replaceAll("(\\[.+\\])", "");
    this.LOG = LoggerFactory.getLogger(name + "-" + jobName);

    this.hostname = NetUtil.getHostname();

    crawlFilters = CrawlFilters.create(conf);
  }

  public static int counterSequence() {
    return counterSequence.get();
  }

  public <T extends Enum<T>> void register(Class<T> counterClass) {
    register(EnumUtils.getEnumMap(counterClass).keySet());
  }

  public final int id() {
    return id;
  }

  public final String name() {
    return name;
  }

  public final String getHostname() {
    return hostname;
  }

  @SuppressWarnings("rawtypes")
  public final TaskInputOutputContext getContext() {
    return context;
  }

  /**
   * TODO : Check thread safety
   * */
  public void increase(Enum<?> counter) {
    increase(getIndex(counter));
  }

  /**
   * TODO : Check thread safety
   * */
  public void increase(Enum<?> counter, int value) {
    increase(getIndex(counter), value);
  }

  /**
   * TODO : Check thread safety
   * */
  public void setValue(Enum<?> counter, int value) {
    setValue(getIndex(counter), value);
  }

  public int getIndex(Enum<?> counter) {
    if (counter instanceof Counter) {
      return countersCount.get() - Counter.values().length + counter.ordinal();
    }

    return counter.ordinal();
  }

  public void updateAffectedRows(String url) {
    // Counters

    if (crawlFilters.veryLikelyBeDetailUrl(url)) {
      increase(getIndex(Counter.detailPages));
    }
    else if (crawlFilters.veryLikelyBeIndexUrl(url)) {
      increase(getIndex(Counter.indexPages));
    }

    increase(getIndex(Counter.totalPages));
  }

  public int get(String name) {
    return get(counterIndexes.get(name));
  }

  public int get(int index) {
    if (!validate(index)) return -1;

    return nativeCounters.get(index).get();
  }

  public int get(Enum<?> counter) {
    return get(getIndex(counter));
  }

  public String getStatusString(String... names) {
    StringBuilder sb = new StringBuilder();

    int nonZeroCounter = 0;
    for (int i = 0; i < countersCount.get(); ++i) {
      String name = counterNames.get(i);

      if (ArrayUtils.isEmpty(names) || ArrayUtils.contains(names, name)) {
        int value = nativeCounters.get(i).get();

        if (value != 0) {
          if (nonZeroCounter++ > 0) {
            sb.append(", ");
          }
          sb.append(name).append(" : ").append(value);
        }        
      }
    }

    return sb.toString();
  }

  public String getStatusString() {
    return getStatusString(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  public void logStatus() {
    String status = getStatusString();

    if (!status.isEmpty()) {
      LOG.info(getStatusString());
    }
    else {
      LOG.info("Nothing counted");
    }
  }

  public void accumulateGlobalCounters() {
    for (int i = 0; i < countersCount.get(); ++i) {
      String name = counterNames.get(i);
      int value = globalCounters.get(i).getAndSet(0);

      if (value != 0) {
        // LOG.debug("global : " + name + " : " + value);
        context.getCounter(counterGroup, name).increment(value);
      }
    }
  }

  protected void increase(int index) {
    if (!validate(index)) {
      return;
    }

    globalCounters.get(index).incrementAndGet();
    nativeCounters.get(index).incrementAndGet();

//    LOG.info("#" + index + " : " + nativeCounters.get(index).get());
  }

  protected void increase(int index, int value) {
    if (!validate(index)) {
      LOG.warn("Failed to increase unknown counter at position " + index);
      return;
    }

    globalCounters.get(index).addAndGet(value);
    nativeCounters.get(index).addAndGet(value);
  }

  protected void increaseAll(int... indexes) {
    for (int index : indexes) {
      increase(index);
    }
  }

  protected void setValue(int index, int value) {
    if (!validate(index)) {
      return;
    }

    globalCounters.get(index).set(value);
    nativeCounters.get(index).set(value);

    // LOG.info("#" + index + " : " + nativeCounters.get(index).get());
  }

  protected void register(Collection<String> counters) {
    if (countersCount.get() != 0) {
      LOG.warn("Already registered");
      return;
    }

    ArrayList<String> allCounters = Lists.newArrayList();
    // User defined counters go first
    allCounters.addAll(counters);
    // Default counters go next
    allCounters.addAll(EnumUtils.getEnumMap(Counter.class).keySet());

    registerCounters(allCounters);
  }

  private void registerCounters(ArrayList<String> names) {
    countersCount.set(names.size());

    for (int i = 0; i < countersCount.get(); ++i) {
      counterNames.add(names.get(i));
      counterIndexes.put(names.get(i), i);
      globalCounters.add(new AtomicInteger(0));
      nativeCounters.add(new AtomicInteger(0));
    }

    if (countersCount.get() == 0) {
      LOG.warn("No counters, will not run report thread");
    }

    Validate.isTrue(counterIndexes.size() == counterNames.size());
    Validate.isTrue(counterIndexes.size() == countersCount.get());
    Validate.isTrue(CollectionUtils.containsAll(counterIndexes.keySet(), counterNames));

    LOG.info("Registered counters : " + StringUtils.join(MapUtils.invertMap(counterIndexes).entrySet(), ", "));
  }

  private boolean validate(int index) {
    if (index < 0 || index >= countersCount.get()) {
      LOG.error("Invalid index #" + index);
      return false;
    }

    return true;
  }
}
