package org.apache.nutch.fetcher.data;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

/**
 * This class described the item to be fetched.
 */
public class FetchItem {
  public static final Logger LOG = FetcherJob.LOG;

  /**
   * The initial value is the current timestamp in second, to make it 
   * an unique id in the current host
   * */
  private static AtomicLong nextFetchItemId = new AtomicLong(System.currentTimeMillis() / 1000);

  public class Key {

    Key(int jobID, String queueID, String url) {
      this.jobID = jobID;
      this.queueID = queueID;
      this.url = url;
    }

    public int jobID;
    public String queueID;
    public long itemID = nextId();
    public String url;
  }

  Key key;
  WebPage page;
  URL u;
  long pendingStart = 0;

  public FetchItem(int jobID, String url, WebPage page, URL u, String queueID) {
    this.page = page;
    this.key = new Key(jobID, queueID, url);
    this.u = u;
  }

  public WebPage getPage() {
    return page;
  }

  public void setPage(WebPage page) {
    this.page = page;
  }

  public String getQueueID() {
    return key.queueID;
  }

  public long getItemID() {
    return key.itemID;
  }

  public String getUrl() {
    return key.url;
  }

  public URL getU() {
    return u;
  }

  public Key getKey() {
    return key;
  }

  public long getPendingStart() {
    return pendingStart;
  }

  public void setPendingStart(long pendingStart) {
    this.pendingStart = pendingStart;
  }

  /** 
   * Create an item. Queue id will be created based on <code>queueMode</code>
   * argument, either as a protocol + hostname pair, protocol + IP
   * address pair or protocol+domain pair.
   */
  public static FetchItem create(int jobID, String url, WebPage page, String queueMode) {
    final URL u = getUrl(url);

    if (u == null) return null;

    final String proto = u.getProtocol().toLowerCase();
    final String host = getHost(u, queueMode).toLowerCase();
    final String queueID = proto + "://" + host;

    return new FetchItem(jobID, url, page, u, queueID);
  }

  /**
   * TODO : is there a common util tool?
   * */
  protected static URL getUrl(String url) {
    URL u = null;

    try {
      u = new URL(url);
    } catch (final Exception e) {
      LOG.warn("Cannot parse url: " + url, e);
      return null;
    }

    return u;
  }

  /**
   * Generate an unique numeric id for the fetch item
   * 
   * fetch item id is used to easy item searching
   * 
   * */
  public long nextId() {
    return nextFetchItemId.incrementAndGet();
  }

  protected static String getHost(URL url, String queueMode) {
    String host;
    if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
      try {
        final InetAddress addr = InetAddress.getByName(url.getHost());
        host = addr.getHostAddress();
      } catch (final UnknownHostException e) {
        // unable to resolve it, so don't fall back to host name
        LOG.warn("Unable to resolve: " + url.getHost() + ", skipping.");
        return null;
      }
    }
    else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)){
      host = URLUtil.getDomainName(url);
      if (host == null) {
        LOG.warn("Unknown domain for url: " + url.toString() + ", using URL string as key");
        host = url.toExternalForm();
      }
    }
    else {
      host = url.getHost();
      if (host == null) {
        LOG.warn("Unknown host for url: " + url.toString() + ", using URL string as key");
        host = url.toExternalForm();
      }
    }

    return host;
  }

  @Override
  public String toString() {
    return "FetchItem [queueID=" + key.queueID + ", id=" + key.itemID + ", url=" + key.url + ", u=" + u
        + ", page=" + page + "]";
  }

}
