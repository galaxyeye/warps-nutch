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
package org.apache.nutch.fetch.service;

import com.google.common.collect.Lists;
import org.apache.nutch.fetch.TaskScheduler;
import org.apache.nutch.fetch.TaskSchedulers;
import org.apache.nutch.fetch.data.FetchTask;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@Path(value = "/fetch")
@Produces({ MediaType.APPLICATION_JSON })
public class FetchResource {

  public static final Logger LOG = LoggerFactory.getLogger(FetchServer.class);

  public final static int MAX_TASKS_PER_SCHEDULE = 100;

  private final TaskSchedulers taskSchedulers = TaskSchedulers.getInstance();

  public FetchResource() {
  }

  @GET
  @Path("/schedule/{count}")
  public List<FetchTask.Key> getFetchItems(@PathParam("count") int count) {
    List<FetchTask.Key> keys = Lists.newArrayList();

    if (count < 0) {
      LOG.debug("Invalid count " + count);
      return keys;
    }

    if (count > MAX_TASKS_PER_SCHEDULE) {
      count = MAX_TASKS_PER_SCHEDULE;
    }

    return taskSchedulers.randomFetchItems(count);
  }

  @GET
  @Path("/scheduler/list")
  public List<Integer> listScheduers() {
    return taskSchedulers.schedulerIds();
  }

  @GET
  @Path("/scheduler/listCommands")
  public List<String> listCommands() {
    return taskSchedulers.schedulerIds().stream()
        .map(id -> "curl http://localhost:8182/fetch/stop/" + id)
        .collect(Collectors.toList());
  }

  @GET
  @Path("/stop/{schedulerId}")
  public void stop(@PathParam("schedulerId") int schedulerId) {
    TaskScheduler taskScheduler = taskSchedulers.get(schedulerId);
    if (taskScheduler != null) {
      taskScheduler.getFetchMonitor().halt();
    }
  }

  @PUT
  @Path("/inject")
  public int inject(String urlLine) {
    int count = 0;

    TaskScheduler taskScheduler = taskSchedulers.peek();
    if (taskScheduler != null) {
      String batchId = "JITInjected" + TimingUtil.now("yyyyMMdd");
      count = taskScheduler.inject(urlLine, batchId);
    }

    return count;
  }

  @PUT
  @Path("/inject")
  @Consumes(MediaType.APPLICATION_JSON)
  public int inject(Set<String> urlLines) {
    int count = 0;

    TaskScheduler taskScheduler = taskSchedulers.peek();
    if (taskScheduler != null) {
      String batchId = "JITInjected" + TimingUtil.now("yyyyMMdd");
      count = taskScheduler.inject(urlLines, batchId);
    }

    return count;
  }

  /**
   * Accept page content from satellite(crowdsourcing web fetcher),
   * the content should be put with media type "text/html; charset='UTF-8'"
   * 
   * TODO : does the framework make a translation between string and byte array?
   * which might affect the performance
   * */
  @PUT
  @Path("/submit")
  @Consumes("text/html; charset='UTF-8'")
  @Produces("text/html; charset='UTF-8'")
  public String finishFetchItem(@javax.ws.rs.core.Context HttpHeaders httpHeaders, byte[] content) {
    Metadata customHeaders = new SpellCheckedMetadata();
    for (Entry<String, List<String>> entry : httpHeaders.getRequestHeaders().entrySet()) {
      String name = entry.getKey().toLowerCase();

      // Q- means meta-data from satellite
      // F- means forwarded headers by satellite
      // ant other headers are information between satellite and this server
      if (name.startsWith("f-") || name.startsWith("q-")) {
        if (name.startsWith("f-")) {
          name = name.substring("f-".length());
        }

        for (String value : entry.getValue()) {
          customHeaders.add(name, value);
        }
      }
    }

//    LOG.debug("headers-1 : {}", httpHeaders.getRequestHeaders().entrySet());
//    LOG.debug("headers-2 : {}", customHeaders);

    FetchResult fetchResult = new FetchResult(customHeaders, content);
    TaskScheduler taskScheduler = taskSchedulers.get(fetchResult.getJobId());
    taskScheduler.produce(fetchResult);

    return "success";
  }
}
