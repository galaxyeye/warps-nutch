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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.util.HadoopFSUtil;
import org.slf4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static javax.ws.rs.core.Response.status;

@javax.ws.rs.Path("/seed")
public class SeedResource extends AbstractResource {
  private static final Logger LOG = AbstractResource.LOG;

  /**
   * Method creates seed list file and returns temorary directory path
   *
   * @param seedList seed list
   * @return String
   */
  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  public String createSeedFile(Collection<String> seedList) {
    if (seedList == null || seedList.isEmpty()) {
      throw new WebApplicationException(
          Response.status(Status.BAD_REQUEST).entity("Seed list cannot be empty!").build());
    }

    try {
      File seedFile = File.createTempFile("seed", ".txt");
      FileUtils.writeLines(seedFile, seedList);

      Configuration conf = configManager.get(ConfManager.DEFAULT);
      if (HadoopFSUtil.isDistributedFS(conf)) {
        LOG.info("Running under hadoop distributed file system");

        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(seedFile.getAbsolutePath());
        fs.copyFromLocalFile(path, path);
      }

      return seedFile.getAbsolutePath();
    } catch (IOException e) {
      LOG.error(e.toString());
      handleException(e);
    }

    return "";
  }

  private RuntimeException handleException(Exception e) {
    LOG.error("Cannot create seed file!", e);
    return new WebApplicationException(status(Status.INTERNAL_SERVER_ERROR)
        .entity("Cannot create seed file!").build());
  }
}
