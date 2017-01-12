package org.apache.nutch.service.model.request;

import java.io.Serializable;

@Deprecated
public class SeedUrl implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private Integer id;

  private String url;

  public SeedUrl() {
  }

  public SeedUrl(int id, String url) {
    this.id = id;
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SeedUrl other = (SeedUrl) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }
}
