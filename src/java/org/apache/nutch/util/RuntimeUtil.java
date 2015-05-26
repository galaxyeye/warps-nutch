package org.apache.nutch.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}
