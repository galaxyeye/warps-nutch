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

package org.apache.nutch.util;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.junit.Test;

/** Test class for URLUtil */
public class TestProcessUtil {

  @Test
  public void test() {
    assertTrue(2 > 1);
  }
  
  public static void main(String[] args) throws Exception {
    int res = 0;

    JSONArray data = new JSONArray(Arrays.asList(args));

    System.out.println("It's crowdsourcing mode, forward the command to NutchServer...");

    ProcessBuilder builder = new ProcessBuilder("curl",
        "-v",
        "-H", "'Content-Type: application/json'",
        "-X", "PUT",
        "--data", data.toString(),
        "http://127.0.0.1:8182/exec/fetch");
    System.out.println("Execute command : " + StringUtils.join(builder.command(), " "));
    builder.inheritIO();
    Process process = builder.start();

    res = process.waitFor();

    System.exit(res);
  }
}
