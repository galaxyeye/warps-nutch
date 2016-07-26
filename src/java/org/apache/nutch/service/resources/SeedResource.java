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

import com.google.common.io.Files;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.model.request.SeedList;
import org.apache.nutch.service.model.request.SeedUrl;
import org.slf4j.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.util.Collection;

import static javax.ws.rs.core.Response.status;

@Path("/seed")
public class SeedResource extends AbstractResource {
  private static final Logger LOG = AbstractResource.LOG;

  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  /**
   * Method creates seed list file and returns temorary directory path
   * @param seedList
   * @return
   */
  public String createSeedFile(SeedList seedList) {
    if (seedList == null) {
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
          .entity("Seed list cannot be empty!").build());
    }

    File seedFile = createSeedFile();
    BufferedWriter writer = getWriter(seedFile);

    Collection<SeedUrl> seedUrls = seedList.getSeedUrls();
    if (CollectionUtils.isNotEmpty(seedUrls)) {
      for (SeedUrl seedUrl : seedUrls) {
        writeUrl(writer, seedUrl);
      }
    }

    try {
      Configuration conf = configManager.get(ConfManager.DEFAULT);
      String fsName = conf.get("fs.default.name");
      LOG.info("fs.default.name : " + fsName);

      if (fsName.contains("hdfs")) {
        LOG.info("Running under hadoop distributed file system");

        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(seedFile.getAbsolutePath());
        fs.copyFromLocalFile(path, path);
      }
      else {
        LOG.info("Running under local file system");
      }
    } catch (IOException e) {
      LOG.error(e.toString());
    }

    return seedFile.getParent();
  }

  private void writeUrl(BufferedWriter writer, SeedUrl seedUrl) {
    try {
      writer.write(seedUrl.getUrl());
      writer.newLine();
      writer.flush();
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private BufferedWriter getWriter(File seedFile) {
    try {
      return new BufferedWriter(new FileWriter(seedFile));
    } catch (FileNotFoundException e) {
      throw handleException(e);
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private File createSeedFile() {
    try {
      return File.createTempFile("seed", ".txt", Files.createTempDir());
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private RuntimeException handleException(Exception e) {
    LOG.error("Cannot create seed file!", e);
    return new WebApplicationException(status(Status.INTERNAL_SERVER_ERROR)
        .entity("Cannot create seed file!").build());
  }

}
