/*
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
package org.apache.nutch.indexwriter.solr;

public interface SolrConstants {

  String SOLR_PREFIX = "solr.";

  String SERVER_URL = SOLR_PREFIX + "server.url";

  String COMMIT_SIZE = SOLR_PREFIX + "commit.size";

  String MAPPING_FILE = SOLR_PREFIX + "mapping.file";

  String USE_AUTH = SOLR_PREFIX + "auth";

  String USERNAME = SOLR_PREFIX + "auth.username";

  String PASSWORD = SOLR_PREFIX + "auth.password";

  String COLLECTION = SOLR_PREFIX + "collection";

  String ZOOKEEPER_HOSTS = SOLR_PREFIX + "zookeeper.hosts";

  String ID_FIELD = "id";

  String URL_FIELD = "url";

  String BOOST_FIELD = "boost";

  String TIMESTAMP_FIELD = "tstamp";

  String DIGEST_FIELD = "digest";

  @Deprecated
  String COMMIT_INDEX = SOLR_PREFIX + "commit.index";

  @Deprecated
  String PARAMS = SOLR_PREFIX + "params";
}
