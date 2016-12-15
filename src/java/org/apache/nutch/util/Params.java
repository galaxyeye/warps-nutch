package org.apache.nutch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by vincent on 16-9-24.
 */
public class Params {
  private final Map<String, Object> paramsMap = new LinkedHashMap<>();
  private String captionFormat = String.format("%20sParams Table%-25s\n", "----------", "----------");
  private String headerFormat = String.format("%25s   %-25s\n", "Name", "Value");
  private String rowFormat = "%25s : %s";
  private Logger logger = LoggerFactory.getLogger(Params.class);

  public Params(String key, Object value, Object... others) {
    this.paramsMap.putAll(toArgMap(key, value, others));
  }

  public Params(Map<String, Object> args) {
    this.paramsMap.putAll(args);
  }

  public String get(String name) {
    Object obj = paramsMap.get(name);
    return obj == null ? null : obj.toString();
  }

  public String get(String name, String defaultValue) {
    String value = (String) paramsMap.get(name);
    return value == null ? defaultValue : value;
  }

  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    String val = this.get(name);
    return null == val ? defaultValue : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  public Integer getInt(String name, Integer defaultValue) {
    Integer value = (Integer) paramsMap.get(name);
    return value == null ? defaultValue : value;
  }

  public Long getLong(String name, Long defaultValue) {
    Long value = (Long) paramsMap.get(name);
    return value == null ? defaultValue : value;
  }

  public Boolean getBoolean(String name, Boolean defaultValue) {
    Boolean value = (Boolean) paramsMap.get(name);
    return value == null ? defaultValue : value;
  }

  public String[] getStrings(String name, String[] defaultValue) {
    String valueString = get(name, null);
    if (valueString == null) {
      return defaultValue;
    }
    return org.apache.hadoop.util.StringUtils.getStrings(valueString);
  }

  public Path getPath(String name) throws IOException {
    String value = get(name);
    if (value == null) return null;

    Path path = Paths.get(value);
    Files.createDirectories(path.getParent());

    return path;
  }

  public Path getPath(String name, Path defaultValue) throws IOException {
    String value = get(name);
    Path path = value == null ? Paths.get(value) : defaultValue;
    Files.createDirectories(path.getParent());
    return path;
  }

  public String format() {
    return formatMap(paramsMap);
  }

  public String formatAsLine() {
    return formatAsLine(paramsMap);
  }

  public Params withCaptionFormat(String captionFormat) {
    this.captionFormat = captionFormat;
    return this;
  }

  public Params withHeaderFormat(String headerFormat) {
    this.headerFormat = headerFormat;
    return this;
  }

  public Params withRowFormat(String rowFormat) {
    this.rowFormat = rowFormat;
    return this;
  }

  public Params merge(Params... others) {
    if (others != null && others.length > 0) {
      Arrays.stream(others).forEach(params -> this.paramsMap.putAll(params.asMap()));
    }
    return this;
  }

  public Map<String, Object> asMap() {
    return paramsMap;
  }

  public Params withLogger(Logger logger) {
    this.logger = logger;
    return this;
  }

  public void debug() {
    if (logger != null) {
      logger.debug(toString());
    }
  }

  public void info() {
    if (logger != null) {
      logger.info(toString());
    }
  }

  public void debug(Logger logger) { withLogger(logger).debug(); }

  public void info(Logger logger) { withLogger(logger).info(); }

  @Override
  public String toString() {
    return format();
  }

  public static Params of(String key, Object value, Object... others) {
    return new Params(key, value, others);
  }

  public static Params of(Map<String, Object> args) {
    return new Params(args);
  }

  /**
   * Convert K/V pairs array into a map.
   *
   * @param others A K/V pairs array, the length of the array must be a even number
   *                null key or null value pair is ignored
   * @return A map contains all non-null key/values
   * */
  public static Map<String, Object> toArgMap(String key, Object value, Object... others) {
    Map<String, Object> results = new LinkedHashMap<>();

    results.put(key, value);

    if (others == null || others.length < 2) {
      return results;
    }

    if (others.length % 2 != 0) {
      throw new RuntimeException("expected name/value pairs");
    }

    for (int i = 0; i < others.length; i += 2) {
      Object k = others[i];
      Object v = others[i + 1];

      if (k != null && v != null) {
        results.put(String.valueOf(others[i]), others[i + 1]);
      }
    }

    return results;
  }

  public static String formatAsLine(String key, Object value, Object... others) {
    return Params.of(key, value, others).formatAsLine();
  }

  public static String format(String key, Object value, Object... others) {
    return Params.of(key, value, others).format();
  }

  private String formatMap(Map<String, Object> params) {
    if (params.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();

    sb.append('\n');
    sb.append(captionFormat);
    sb.append(headerFormat);
    int i = 0;
    for (Map.Entry<String, Object> arg : params.entrySet()) {
      if (i++ > 0) {
        sb.append("\n");
      }

      sb.append(String.format(rowFormat, arg.getKey(), arg.getValue()));
    }

    sb.append('\n');

    return sb.toString();
  }

  private String formatAsLine(Map<String, Object> params) {
    if (params.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();

    int i = 0;
    for (Map.Entry<String, Object> arg : params.entrySet()) {
      if (i++ > 0) {
        sb.append(", ");
      }

      sb.append(arg.getKey());
      sb.append(" : ");
      sb.append(arg.getValue());
    }

    return sb.toString();
  }
}
