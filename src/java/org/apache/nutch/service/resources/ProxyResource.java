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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;

import org.apache.nutch.service.model.request.ProxyConfig;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.proxy.ProxyPool;
import org.restlet.Request;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.slf4j.Logger;

import com.google.gson.Gson;

/**
 * Proxy Resource module
 * */
@Path("/proxy")
public class ProxyResource extends AbstractResource {
  private static final Logger LOG = AbstractResource.LOG;

  public static final String PATH = "proxy";
  public static final String DESCR = "Proxy server resources";

  TestAndSaveThread testAndSaveThread = null;

  /**
   * List all proxy servers
   * */
  @GET
  @Path("/")
  public List<String> list() {
    return ProxyPool.getConfiguredProxyList();
  }

  @PUT
  @Path("/echo")
  @Consumes("text/html; charset='UTF-8'")
  @Produces("text/html; charset='UTF-8'")
  public byte[] echo(@javax.ws.rs.core.Context HttpHeaders httpHeaders, byte[] content) {
    Metadata customHeaders = new SpellCheckedMetadata();
    for (java.util.Map.Entry<String, List<String>> entry : httpHeaders.getRequestHeaders().entrySet()) {
      for (String value : entry.getValue()) {
        customHeaders.add(entry.getKey(), value);
      }
    }

    LOG.info("{}", httpHeaders.getRequestHeaders().entrySet());
    LOG.info("{}", customHeaders);

//    debugContent("1", content);
//    debugContent("2", Bytes.toString(content).array());

    return content;
  }

  /**
   * download proxy list file as a text file
   * */
  @GET
  @Path("/download")
  public Representation download() {
    return new FileRepresentation(ProxyPool.ProxyListFile, org.restlet.data.MediaType.TEXT_PLAIN);
  }

  /**
   * refresh proxy list file
   * */
  @GET
  @Path("/touch")
  public String touch() {
    return "last modified : " + ProxyPool.touchProxyConfigFile();
  }

  /**
   * manage reports from satellite
   * 
   * satellite reports itself with it's configuration file, 
   * the server side add the satellite into proxy list
   * 
   * */
  @POST
  @Path("/report")
  @Consumes(javax.ws.rs.core.MediaType.APPLICATION_JSON)
  public ArrayList<String> report(String content) {
    String host = Request.getCurrent().getClientInfo().getAddress();

    // logger.info("received report from " + host);
    // logger.debug("received content : {} ", content);

    Gson gson = new Gson();
    ProxyConfig proxyConfig = gson.fromJson(content, ProxyConfig.class);

    ArrayList<String> proxyList = new ArrayList<String>();
    if (proxyConfig != null) {
      for (int i = 0; i < proxyConfig.coordinator.proxyProcessCount; ++i) {
        int port = proxyConfig.coordinator.serverPortBase + i;
        proxyList.add(host + ":" + port);
      }
    }

    // logger.debug("received proxy server list : {} ", proxyList);

    testAndSaveThread = new TestAndSaveThread(proxyList);
    testAndSaveThread.start();

    return proxyList;
  }

  private class TestAndSaveThread extends Thread {
    private final List<String> proxyList;

    TestAndSaveThread(List<String> proxyList) {
      this.proxyList = proxyList;
    }

    @Override
    public void start() {
      setName(getClass().getSimpleName());
      setDaemon(true);
      super.start();
    }

    @Override
    public void run() {
      if (proxyList != null && !proxyList.isEmpty()) {
        try {
          ProxyPool.testAndSave(proxyList);
        } catch (IOException e) {
          LOG.error(e.toString());
        }
      }
    }
  }
}
