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
package org.apache.nutch.service.resources;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.SetUtils;
import org.apache.nutch.common.Params;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.impl.db.Db;
import org.apache.nutch.service.impl.db.DbIterator;
import org.apache.nutch.service.model.request.DbQuery;
import org.apache.nutch.service.model.response.DbQueryResult;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID_STR;

@Path("/db")
public class DbResource extends AbstractResource {

  private final Map<String, Db> readers = new WeakHashMap<>();

  @GET
  @Path("/echo")
  public List<String> echo(@QueryParam("crawlId") String crawlId, @QueryParam("dbUrl") String url) {
    return Lists.newArrayList(crawlId, url);
  }

  @GET
  @Path("/get")
  public DbQueryResult get(@QueryParam("crawlId") String crawlId, @QueryParam("dbUrl") String url) {
    DbQueryResult result = new DbQueryResult();

    Params.of("crawlId", crawlId, "url", url).withLogger(LOG).info(true);

    Db db = getDb(ConfManager.DEFAULT, crawlId);

    result.addValue(db.get(url, SetUtils.emptySet()));

    return result;
  }

  /**
   * Remove seed
   * @param crawlId crawlId
   * @param url url to delete
   * @return boolean
   */
  @DELETE
  @Path("/delete")
  public boolean remove(@QueryParam("crawlId") String crawlId, @QueryParam("dbUrl") String url) {
    Db db = getDb(ConfManager.DEFAULT, crawlId);
    return db.delete(url);
  }

  @GET
  @Path("/scan")
  public DbQueryResult scan(@QueryParam("crawlId") String crawlId,
                            @QueryParam("startUrl") String startUrl,
                            @QueryParam("endUrl") String endUrl) {
    DbQueryResult result = new DbQueryResult();
    DbQuery filter = new DbQuery(crawlId, ALL_BATCH_ID_STR, startUrl, endUrl);

    Params.of("crawlId", crawlId, "startUrl", startUrl, "endUrl", endUrl).withLogger(LOG).info(true);
    // LOG.info();

    Db db = getDb(ConfManager.DEFAULT, crawlId);
    DbIterator iterator = db.query(filter);

    long ignoreCount = 0L;
    while (++ignoreCount < filter.getStart() && iterator.hasNext()) {
      iterator.skipNext();
    }

    while (iterator.hasNext()) {
      result.addValue(iterator.next());
    }

    return result;
  }

  @POST
  @Path("/query")
  @Consumes(MediaType.APPLICATION_JSON)
  public DbQueryResult query(DbQuery query) {
    DbQueryResult result = new DbQueryResult();
    Db db = getDb(ConfManager.DEFAULT, query.getCrawlId());
    DbIterator iterator = db.query(query);

    long ignoreCount = 0L;
    while (++ignoreCount < query.getStart() && iterator.hasNext()) {
      iterator.skipNext();
    }

    while (iterator.hasNext()) {
      result.addValue(iterator.next());
    }

    return result;
  }

  private Db getDb(String configId, String crawlId) {
    synchronized (readers) {
      if (!readers.containsKey(configId)) {
        readers.put(configId, new Db(configManager.get(configId), crawlId));
      }

      return readers.get(configId);
    }
  }
}
