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
package org.apache.nutch.indexer;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestIndexingFilters {

  @Before
  public void setup() {
//    System.out.println(System.getenv("PATH"));
//    System.out.println(System.getenv("PWD"));
  }

  /**
   * Test behaviour when defined filter does not exist.
   * 
   * @throws IndexingException
   */
  @Test
  public void testNonExistingIndexingFilter() throws IndexingException, MalformedURLException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    String class1 = "NonExistingFilter";
    String class2 = "org.apache.nutch.indexer.basic.BasicIndexingFilter";
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1 + " " + class2);

    IndexingFilters filters = new IndexingFilters(conf);
    WebPage page = WebPage.newBuilder().build();
    page.setText(new Utf8("text"));
    page.setTitle(new Utf8("title"));

    String url = "http://www.example.com/";
    String key = TableUtil.reverseUrl(url);
    filters.filter(new IndexDocument(key), url, page);
  }

  /**
   * Test behaviour when NutchDOcument is null
   *
   * @throws IndexingException
   */
  @Test
  public void testNutchDocumentNullIndexingFilter() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    IndexingFilters filters = new IndexingFilters(conf);
    WebPage page = WebPage.newBuilder().build();
    page.setText(new Utf8("text"));
    page.setTitle(new Utf8("title"));
    IndexDocument doc = filters.filter(null, "http://www.example.com/", page);

    assertNull(doc);
  }

  /**
   * Test behaviour when reset the index filter order will not take effect
   * 
   * @throws IndexingException
   */
  @Test
  public void testFilterCacheIndexingFilter() throws IndexingException, MalformedURLException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    String class1 = "org.apache.nutch.indexer.basic.BasicIndexingFilter";
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1);

    String url = "http://www.example.com/";
    String key = TableUtil.reverseUrl(url);

    IndexingFilters filters1 = new IndexingFilters(conf);
    WebPage page = WebPage.newBuilder().build();
    page.setText(new Utf8("text"));
    page.setTitle(new Utf8("title"));

    IndexDocument fdoc1 = filters1.filter(new IndexDocument(key), url, page);

    // add another index filter
    String class2 = "org.apache.nutch.indexer.metadata.MetadataIndexer";
    page.getMetadata().put("example", ByteBuffer.wrap(Bytes.toBytes("data")));
    // add MetadataIndxer filter
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1 + " " + class2);
    conf.set("index.metadata", "example");
    IndexingFilters filters2 = new IndexingFilters(conf);

    IndexDocument fdoc2 = filters2.filter(new IndexDocument(key), url, page);

//    System.out.println(".....................");
//
//    System.out.println(conf.get("plugin.includes"));
//
//    System.out.println(filters1);
//    System.out.println(filters2);
//
//    System.out.println(fdoc1);
//    System.out.println(fdoc2);

    assertEquals(fdoc1.getFieldNames().size(), fdoc2.getFieldNames().size());
  }

  /**
   * Test behaviour when NutchDOcument is null
   *
   * @throws IndexingException
   */
  @Test
  public void testIndexingFilterRealWordDoc() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    IndexingFilters filters = new IndexingFilters(conf);
    WebPage page = WebPage.newBuilder().build();
    page.setText(new Utf8("text"));
    page.setTitle(new Utf8("title"));
    IndexDocument doc = filters.filter(null, "http://www.example.com/", page);

    assertNull(doc);
  }
}
