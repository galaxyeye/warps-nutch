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

// CURRENTLY DISABLED. TESTS ARE FLAPPING FOR NO APPARENT REASON.
// SHALL BE FIXED OR REPLACES BY NEW API IMPLEMENTATION

package org.apache.nutch.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nutch.persist.gora.db.DbQuery;
import org.apache.nutch.service.model.request.JobConfig;
import org.junit.Test;

import static org.apache.nutch.metadata.Nutch.ARG_SEED_PATH;
import static org.junit.Assert.assertEquals;

public class TestAPISerialization {

  @Test
  public void testJobConfig() {
    JobConfig jobConfig = new JobConfig("test_resource", "test_resource", JobManager.JobType.INJECT);
    jobConfig.getArgs().put(ARG_SEED_PATH, "/tmp/1484315118872-0/");
    Gson gson = new GsonBuilder().create();
    assertEquals("{\"crawlId\":\"test_resource\",\"confId\":\"test_resource\",\"type\":\"INJECT\",\"args\":{\"seedDir\":\"/tmp/1484315118872-0/\"}}", gson.toJson(jobConfig));
  }

  @Test
  public void testDbFilter() {
    DbQuery dbQuery = new DbQuery("http://www.warpspeed.cn/0", "http://www.warpspeed.cn/3");
    Gson gson = new GsonBuilder().create();
    assertEquals("{\"crawlId\":\"\",\"batchId\":\"-all\",\"startUrl\":\"http://www.warpspeed.cn/0\",\"endUrl\":\"http://www.warpspeed.cn/3\",\"urlFilter\":\"+.\",\"start\":0,\"limit\":100,\"fields\":[]}", gson.toJson(dbQuery));

    DbQuery dbQuery2 = gson.fromJson("{\"startUrl\":\"http://www.warpspeed.cn/0\",\"endUrl\":\"http://www.warpspeed.cn/3\"}", DbQuery.class);
    assertEquals("", dbQuery2.getCrawlId());
    assertEquals("-all", dbQuery2.getBatchId());
    assertEquals("http://www.warpspeed.cn/0", dbQuery2.getStartUrl());
    assertEquals("http://www.warpspeed.cn/3", dbQuery2.getEndUrl());
  }
}
