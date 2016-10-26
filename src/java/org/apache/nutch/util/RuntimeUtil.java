package org.apache.nutch.util;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class RuntimeUtil {

  protected static final Logger logger = LoggerFactory.getLogger(RuntimeUtil.class);

  public static boolean checkIfJavaProcessRunning(String imageName) {
    try {
      Process proc = Runtime.getRuntime().exec("jps");
      BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line = null;
      while ((line = in.readLine()) != null) {
        // System.out.println(line);
        if (line.contains(imageName)) {
          return true;
        }
      }
    } catch (Exception e) {
      logger.error(e.toString());
    }

    return false;
  }

  public static void main(String[] args) throws Exception {
    int res = 0;

    JSONArray data = new JSONArray(Arrays.asList(args));

    System.out.println("It's crowdsourcing mode, forward the command to NutchMaster...");

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
