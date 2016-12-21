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
package org.apache.nutch.persist.local;

import java.util.List;

import org.apache.nutch.persist.local.model.ServerInstance;
import org.apache.nutch.persist.local.service.ServerInstanceService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Tests basic Gora functionality by writing and reading webpages.
 */
public class TestLocalStorage {

  private ApplicationContext springContext;

  @Autowired
  private ServerInstanceService serverInstanceService;

  @Before
  public void setUp() {
    springContext = new AnnotationConfigApplicationContext(SpringConfiguration.class);
    // ServerInstanceService serverInstanceService = springContext.getBean(ServerInstanceService.class);
  }

  @Test
  public void testList() {
    List<ServerInstance> serverInstances = serverInstanceService.list();
    System.out.println(serverInstances);
  }

  @After
  public void tearDown() {
  }

  public static void main(String[] args) {
    // entry point for the multiprocess test
    System.out.println("Starting!");

    System.out.println("Done.");
  }
}
