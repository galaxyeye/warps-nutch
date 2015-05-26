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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;

public class EnvironmentUtil {

  public static void checker(String[] args) throws IOException {
    System.out.println("Charset : " + Charset.defaultCharset().displayName());
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Charset : " + Charset.defaultCharset().displayName());

    if (args.length < 1) return;

    String txt = FileUtils.readFileToString(new File(args[0]), "UTF-8");
    System.out.println(txt);
  }
}
