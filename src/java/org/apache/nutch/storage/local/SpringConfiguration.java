/**
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
package org.apache.nutch.storage.local;

import java.sql.SQLException;
import java.util.concurrent.Executor;

import org.apache.nutch.storage.local.model.BrowserInstance;
import org.apache.nutch.storage.local.model.SeedList;
import org.apache.nutch.storage.local.model.SeedUrl;
import org.apache.nutch.storage.local.model.ServerInstance;
import org.apache.nutch.storage.local.service.BrowserInstanceService;
import org.apache.nutch.storage.local.service.ServerInstanceService;
import org.apache.nutch.storage.local.service.impl.BrowserInstanceServiceImpl;
import org.apache.nutch.storage.local.service.impl.ServerInstanceServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.db.H2DatabaseType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;

@Configuration
@EnableAsync
public class SpringConfiguration implements AsyncConfigurer {

  public Executor getAsyncExecutor() {
    // TODO move magic numbers to properties file
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(7);
    executor.setMaxPoolSize(42);
    executor.setQueueCapacity(11);
    executor.setThreadNamePrefix("SpringExecutor-");
    executor.initialize();
    return executor;
  }

  @Bean
  public JdbcConnectionSource getConnectionSource() throws SQLException {
    JdbcConnectionSource source = new JdbcConnectionSource("jdbc:h2:~/.nutch/nutch", new H2DatabaseType());
    source.initialize();

    return source;
  }

  @Bean
  public CustomDaoFactory getDaoFactory() throws SQLException {
    return new CustomDaoFactory(getConnectionSource());
  }

  @Bean
  public Dao<SeedList, Long> createSeedListDao() throws SQLException {
    return getDaoFactory().createDao(SeedList.class);
  }

  @Bean
  public Dao<SeedUrl, Long> createSeedUrlDao() throws SQLException {
    return getDaoFactory().createDao(SeedUrl.class);
  }

  @Bean
  public Dao<ServerInstance, Long> createServerInstanceDao() throws SQLException {
    return getDaoFactory().createDao(ServerInstance.class);
  }

  @Bean
  public Dao<BrowserInstance, Long> createBrowserInstanceDao() throws SQLException {
    return getDaoFactory().createDao(BrowserInstance.class);
  }

  @Bean
  public ServerInstanceService createServerInstanceService() {
    return new ServerInstanceServiceImpl();
  }

  @Bean
  public BrowserInstanceService createBrowserInstanceService() {
    return new BrowserInstanceServiceImpl();
  }

  @Bean
  public CustomTableCreator createTableCreator() throws SQLException {
    return new CustomTableCreator(getConnectionSource(), getDaoFactory().getCreatedDaos());
  }
}
