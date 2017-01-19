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
package org.apache.nutch.persist.gora.db;

import java.util.HashSet;
import java.util.Set;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID_STR;

public class DbQuery {

  private String crawlId = "";
  private String batchId = ALL_BATCH_ID_STR;
  private String startUrl;
  private String endUrl;
  private String urlFilter = "+.";
  private Long start = 0L;
  private Long limit = 100L;
  private Set<String> fields = new HashSet<>();

  private DbQuery() {}

  public DbQuery(String startUrl, String endUrl) {
    this.startUrl = startUrl;
    this.endUrl = endUrl;
  }

  public DbQuery(String crawlId, String batchId, String startUrl, String endUrl) {
    this.crawlId = crawlId;
    this.batchId = batchId;
    this.startUrl = startUrl;
    this.endUrl = endUrl;
  }

  public String getCrawlId() {
    return crawlId == null ? "" : crawlId;
  }
  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }
  public String getBatchId() {
    return batchId == null ? ALL_BATCH_ID_STR : batchId;
  }
  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }
  public String getStartUrl() {
    return startUrl;
  }
  public void setStartUrl(String startUrl) {
    this.startUrl = startUrl;
  }
  public String getEndUrl() {
    return endUrl;
  }
  public void setEndUrl(String endUrl) {
    this.endUrl = endUrl;
  }
  public String getUrlFilter() {
    return urlFilter;
  }
  public void setUrlFilter(String urlFilter) {
    this.urlFilter = urlFilter;
  }
  public Long getStart() {
    return start;
  }
  public void setStart(Long start) {
    this.start = start;
  }
  public Long getLimit() {
    return limit;
  }
  public void setLimit(Long limit) {
    this.limit = limit;
  }
  public Set<String> getFields() { return fields; }
  public void setFields(Set<String> fields) {
    this.fields = fields;
  }
}
