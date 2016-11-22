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
package org.apache.nutch.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class HadoopFSUtil {

  /**
   * Check if running under distributed file system
   * */
  public static boolean isDistributedFS(Configuration conf) {
    String fsName = conf.get("fs.defaultFS");
    return fsName != null && fsName.startsWith("hdfs");
  }

  public static Set<String> loadSeeds(String path, String jobName, Configuration conf) throws IOException {
    Set<String> seedUrls = new HashSet<>();

    if (isDistributedFS(conf)) {
      // read all lines and lock the file, any other progress can not read the file if the lock file does not exist
      List<String> readedSeedUrls = readAndLock(new Path("hdfs://" + path), jobName, conf);
      // Collections.shuffle(readedSeedUrls);
      // TODO : since seeds have higher priority, we still need an algorithm to drop out the pages who does not change
      // Random select a sub set of all urls, no more than 5000 because of the efficiency problem
      // readedSeedUrls = readedSeedUrls.subList(0, 1000);
      seedUrls.addAll(readedSeedUrls);

      NutchUtil.LOG.info("Loaded " + seedUrls.size() + " seed urls");
    }

    // Testing
    seedUrls.add("http://www.sxrb.com/sxxww/xwpd/sx/");
//    seedUrls.add("http://news.baidu.com/");

    return seedUrls;
  }

  /**
   * Returns PathFilter that passes all paths through.
   */
  public static PathFilter getPassAllFilter() {
    return new PathFilter() {
      public boolean accept(Path arg0) {
        return true;
      }
    };
  }

  /**
   * Returns PathFilter that passes directories through.
   */
  public static PathFilter getPassDirectoriesFilter(final FileSystem fs) {
    return new PathFilter() {
      public boolean accept(final Path path) {
        try {
          return fs.getFileStatus(path).isDirectory();
        } catch (IOException ioe) {
          return false;
        }
      }
    };
  }

  /**
   * Turns an array of FileStatus into an array of Paths.
   */
  public static Path[] getPaths(FileStatus[] stats) {
    if (stats == null) {
      return null;
    }
    if (stats.length == 0) {
      return new Path[0];
    }
    Path[] res = new Path[stats.length];
    for (int i = 0; i < stats.length; i++) {
      res[i] = stats[i].getPath();
    }
    return res;
  }

  public static boolean createIfNotExits(Path path, Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(conf);

      if (fs.exists(path)) {
        // The lock file exists, someone else have already read it
        return false;
      } else {
        // The lock file does not exist, i will read it
        FSDataOutputStream out = fs.create(path);
        out.close();
        return true;
      }
    } catch (IOException e) {
      NutchUtil.LOG.error(e.toString());
    }

    return false;
  }

  public static boolean deleteIfExits(Path path, Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(conf);

      if (fs.exists(path)) {
        fs.delete(path, false);
      }
    } catch (IOException e) {
      NutchUtil.LOG.warn(e.toString());
    }

    return false;
  }

  public static boolean isLocked(Path path, String jobName, Configuration conf) {
    Path lockPath = new Path(path.toString() + "." + jobName + ".lock");

    try (FileSystem fs = FileSystem.get(conf)) {
      return fs.exists(lockPath);
    } catch (IOException e) {
      NutchUtil.LOG.error(e.toString());
    }

    return false;
  }

  public static boolean lock(Path path, String jobName, Configuration conf) {
    Path lockPath = new Path(path.toString() + "." + jobName + ".lock");
    createIfNotExits(lockPath, conf);
    return true;
  }

  public static boolean unlock(Path path, String jobName, Configuration conf) {
    Path lockPath = new Path(path.toString() + "." + jobName + ".lock");
    deleteIfExits(lockPath, conf);
    return true;
  }

  public static List<String> readAndLock(Path path, String jobName, Configuration conf) {
    if (isLocked(path, jobName, conf)) {
      return new ArrayList<>();
    }

    lock(path, jobName, conf);
    return readAllLines(path, conf);
  }

  public static List<String> readAndUnlock(Path path, String jobName, Configuration conf) {
    List<String> seeds = readAllLines(path, conf);
    unlock(path, jobName, conf);
    return seeds;
  }

  public static List<String> readIfNotLocked(Path path, String jobName, Configuration conf) {
    if (isLocked(path, jobName, conf)) {
      return new ArrayList<>();
    }

    return readAllLines(path, conf);
  }

  public static List<String> readAllLines(Path path, Configuration conf) {
    List<String> seeds = new ArrayList<>();

    try {
      FileSystem fs = FileSystem.get(conf);

      if (!fs.exists(path)) {
        NutchUtil.LOG.warn("Seed file does not exit!!!");
        return seeds;
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
        String line = reader.readLine();
        while (line != null) {
          if (line.startsWith("http")) {
            seeds.add(line);
          }

          line = reader.readLine();
        } // while
      }
    } catch (IOException e) {
      NutchUtil.LOG.error(e.toString());
    }

    return seeds;
  }
}
