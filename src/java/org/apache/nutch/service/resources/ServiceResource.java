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
import org.apache.nutch.persist.local.model.BrowserInstance;
import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.persist.local.service.BrowserInstanceService;
import org.apache.nutch.persist.local.service.ServerInstanceService;
import org.apache.nutch.util.NetUtil;
import org.restlet.Request;
import org.restlet.data.ClientInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * SimpleFetcher Server Resource
 * */
@Path("/service")
public class ServiceResource extends AbstractResource {

  private static final Logger logger = LoggerFactory.getLogger(ServiceResource.class);

  private ServerInstanceService serverInstanceService;
  private BrowserInstanceService browserInstanceService;

  private static long lastCheckAvailableTime = System.currentTimeMillis();

  public ServiceResource() {
    serverInstanceService = springContext.getBean(ServerInstanceService.class);
    browserInstanceService = springContext.getBean(BrowserInstanceService.class);
  }

  /**
   * List all servers instances
   * */
  @GET
  @Path("/")
  public List<ServerInstance> list(@QueryParam("type") String type) {
    if (type == null) {
      return serverInstanceService.list();
    }

    ServerInstance.Type t = ServerInstance.Type.FetcherServer;
    if (type.equals(ServerInstance.Type.NutchServer.name())) {
      t = ServerInstance.Type.NutchServer;
    }

    return getLiveServerInstances(t);
  }

  /**
   * List all servers instances
   * */
  @GET
  @Path("/")
  @Consumes(javax.ws.rs.core.MediaType.APPLICATION_JSON)
  public List<ServerInstance> list(BrowserInstance browserInstance) {
    if (browserInstanceService.authorize(browserInstance)) {
      return getLiveServerInstances(ServerInstance.Type.FetcherServer);      
    }

    return Lists.newArrayList();
  }

  @POST
  @Path("/login")
  @Consumes(javax.ws.rs.core.MediaType.APPLICATION_JSON)
  public BrowserInstance report(BrowserInstance browserInstance) {
    if (browserInstance.getId() == null) {
      browserInstance.setCreated(Request.getCurrent().getDate());
    }

    ClientInfo clientInfo = Request.getCurrent().getClientInfo();
    browserInstance.setIp(clientInfo.getAddress());
    browserInstance.setUserAgent(clientInfo.getAgent());
    browserInstance.setSesssion("session");
    browserInstance.setModified(Request.getCurrent().getDate());
    browserInstanceService.save(browserInstance);

    return browserInstance;
  }

  /**
   * Register nutch relative server
   * */
  @POST
  @Path("/register")
  @Consumes(MediaType.APPLICATION_JSON)
  public ServerInstance register(ServerInstance serverInstance) {
    String ip = Request.getCurrent().getClientInfo().getAddress();
    serverInstance.setIp(ip);
    return serverInstanceService.register(serverInstance);
  }

  /**
   * Unregister nutch relative server
   * */
  @DELETE
  @Path("/unregister")
  @Consumes(MediaType.APPLICATION_JSON)
  public void unregister(ServerInstance serverInstance) {
    serverInstanceService.unregister(serverInstance.getId());
  }

  private List<ServerInstance> getLiveServerInstances(ServerInstance.Type type) {
    List<ServerInstance> serverInstances = serverInstanceService.list(type);

    long now = Request.getCurrent().getDate().getTime();
    int checkPeriod = configManager.getDefault().getInt("service.resource.check.period.sec", 15);
    if (now - lastCheckAvailableTime > checkPeriod * 1000) {
      List<ServerInstance> serverInstances2 = Lists.newArrayList();

      // LOG.debug("Service resource check time : " + Request.getCurrent().getDate());

      for (ServerInstance serverInstance : serverInstances) {
        if (!NetUtil.testNetwork(serverInstance.getIp(), serverInstance.getPort())) {
          serverInstanceService.unregister(serverInstance.getId());
        }
        else {
          serverInstances2.add(serverInstance);
        }
      }

      lastCheckAvailableTime = now;

      return serverInstances2;
    }

//    System.out.println(serverInstances);
//    System.out.println(now);
//    System.out.println(now - lastCheckAvailableTime);

    return serverInstances;
  }
}
