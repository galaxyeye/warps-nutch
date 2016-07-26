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
package org.apache.nutch.service.model.request;

import org.apache.nutch.metadata.Nutch;

import java.util.Set;

public class DbFilter {
  public static Long DefaultDbLimit = 100L;

  private String crawlId = null;
  private String batchId = Nutch.ALL_BATCH_ID_STR;
  private String startKey;
  private String endKey;
  private String urlFilter = ".+";
  private Long start = 0L;
  private Long limit = DefaultDbLimit;
  private boolean isKeysReversed = false;
  private Set<String> fields;

  public String getCrawlId() {
    return crawlId;
  }
  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }
  public String getBatchId() {
    return batchId;
  }
  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }
  public String getStartKey() {
    return startKey;
  }
  public void setStartKey(String startKey) {
    this.startKey = startKey;
  }
  public String getEndKey() {
    return endKey;
  }
  public void setEndKey(String endKey) {
    this.endKey = endKey;
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
  public boolean isKeysReversed() {
    return isKeysReversed;
  }
  public void setKeysReversed(boolean isKeysReversed) {
    this.isKeysReversed = isKeysReversed;
  }
  public Set<String> getFields() {
    return fields;
  }
  public void setFields(Set<String> fields) {
    this.fields = fields;
  }
}
