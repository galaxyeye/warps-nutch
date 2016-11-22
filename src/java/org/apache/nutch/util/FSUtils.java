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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for common filesystem operations.
 */
public class FSUtils {

  /**
   * Replaces the current path with the new path and if set removes the old
   * path. If removeOld is set to false then the old path will be set to the
   * name current.old.
   *
   * @param fs          The FileSystem.
   * @param current     The end path, the one being replaced.
   * @param replacement The path to replace with.
   * @param removeOld   True if we are removing the current path.
   * @throws IOException If an error occurs during replacement.
   */
  public static void replace(FileSystem fs, Path current, Path replacement, boolean removeOld) throws IOException {
    // rename any current path to old
    Path old = new Path(current + ".old");
    if (fs.exists(current)) {
      fs.rename(current, old);
    }

    // rename the new path to current and remove the old path if needed
    fs.rename(replacement, current);
    if (fs.exists(old) && removeOld) {
      fs.delete(old, true);
    }
  }

  /**
   * Closes a group of SequenceFile readers.
   *
   * @param readers The SequenceFile readers to close.
   * @throws IOException If an error occurs while closing a reader.
   */
  public static void closeReaders(SequenceFile.Reader[] readers)
      throws IOException {

    // loop through the readers, closing one by one
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        SequenceFile.Reader reader = readers[i];
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  /**
   * Closes a group of MapFile readers.
   *
   * @param readers The MapFile readers to close.
   * @throws IOException If an error occurs while closing a reader.
   */
  public static void closeReaders(MapFile.Reader[] readers) throws IOException {

    // loop through the readers closing one by one
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        MapFile.Reader reader = readers[i];
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  public static boolean isDistributedFS(Configuration conf) {
    String fsName = conf.get("fs.defaultFS");
    return fsName != null && fsName.startsWith("hdfs");
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
