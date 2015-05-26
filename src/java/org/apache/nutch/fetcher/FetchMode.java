package org.apache.nutch.fetcher;

/**
 * TODO : use just enum FetchMode {NATIVE, PROXY, CROWDSOURCING};
 * */
public enum FetchMode {
  NATIVE("native"), PROXY("proxy"), CROWDSOURCING("crowdsourcing");

  private final String value;

  FetchMode(String value) {
    this.value = value.toLowerCase();
  }

  public String value() {
    return value;
  }

  public boolean equals(String mode) {
    return value.equals(mode);
  }

  public boolean equalsIgnoreCase(String mode) {
    return value.equals(mode.toLowerCase());
  }

  public static FetchMode fromString(String mode) {
    if (mode.equalsIgnoreCase("native")) return NATIVE;
    if (mode.equalsIgnoreCase("proxy")) return PROXY;
    if (mode.equalsIgnoreCase("crowdsourcing")) return CROWDSOURCING;

    return NATIVE;
  }
}
