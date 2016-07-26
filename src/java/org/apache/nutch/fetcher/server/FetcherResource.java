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
package org.apache.nutch.fetcher.server;

import com.google.common.collect.Lists;
import org.apache.nutch.fetcher.FetchManager;
import org.apache.nutch.fetcher.FetchManagerPool;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.fetcher.data.FetchItem;
import org.apache.nutch.fetcher.data.FetchResult;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.slf4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path(value = "/fetch")
@Produces({ MediaType.APPLICATION_JSON })
public class FetcherResource {

  public static final Logger LOG = FetcherJob.LOG;

  public final static int MAX_TASKS_PER_SCHEDULE = 100;

  private final FetchManagerPool fetchManagerPool = FetchManagerPool.getInstance();

  public FetcherResource() {
  }

  @GET
  @Path("/schedule/{count}")
  public List<FetchItem.Key> getFetchItems(@PathParam("count") int count) {
    List<FetchItem.Key> keys = Lists.newArrayList();

    if (count < 0) {
      LOG.debug("Invalid count " + count);
      return keys;
    }

    if (count > MAX_TASKS_PER_SCHEDULE) {
      count = MAX_TASKS_PER_SCHEDULE;
    }

    return fetchManagerPool.randomFetchItems(count);
  }

//  @PUT
//  @Path("/inject/{url}")
//  public List<FetchItem.Key> addFetchItem(@PathParam("url") String item) {
//    List<FetchItem.Key> keys = Lists.newArrayList();
//
//    if (count < 0) {
//      LOG.debug("Invalid count " + count);
//      return keys;
//    }
//
//    fetchManagerPool.get();
//
//    return keys;
//  }

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
    for (java.util.Map.Entry<String, List<String>> entry : httpHeaders.getRequestHeaders().entrySet()) {
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
    FetchManager fetchManager = fetchManagerPool.get(fetchResult.getJobId());
    fetchManager.produceFetchResut(fetchResult);

    return "success";
  }
}
