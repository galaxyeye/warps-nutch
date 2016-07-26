package org.apache.nutch.fetcher;

/**
 * TODO : use just enum FetchMode {NATIVE, PROXY, CROWDSOURCING};
 * */
public enum FetchMode {
  UNKNOWN("unknown"), NATIVE("native"), PROXY("proxy"), CROWDSOURCING("crowdsourcing");

  private final String value;

  FetchMode(String value) {
    this.value = value.toLowerCase();
  }

  public String value() {
    return value;
  }

  public boolean equals(String mode) {
    return value.equalsIgnoreCase(mode);
  }

  public boolean equals(FetchMode mode) {
    return value.equals(mode.value);
  }

  public static FetchMode fromString(String mode) {
    if (mode.equalsIgnoreCase("native")) return NATIVE;
    if (mode.equalsIgnoreCase("proxy")) return PROXY;
    if (mode.equalsIgnoreCase("crowdsourcing")) return CROWDSOURCING;

    return UNKNOWN;
  }

  public static boolean validate(String str) {
    if (str.equalsIgnoreCase("native")) return true;
    if (str.equalsIgnoreCase("proxy")) return true;
    if (str.equalsIgnoreCase("crowdsourcing")) return true;

    return false;
  }
}
