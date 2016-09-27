package org.apache.nutch.fetch.data;

import org.apache.nutch.mapreduce.FetchJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nutch.fetch.data.FetchQueues.QUEUE_MODE_DOMAIN;
import static org.apache.nutch.fetch.data.FetchQueues.QUEUE_MODE_IP;

/**
 * This class described the item to be fetched.
 */
public class FetchTask implements Comparable<FetchTask> {
  public static final Logger LOG = FetchJob.LOG;

  /**
   * The initial value is the current timestamp in second, to make it 
   * an unique id in the current host
   * */
  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  public class Key implements Comparable<Key> {

    Key(int jobID, String queueID, String url) {
      this.jobID = jobID;
      this.queueID = queueID;
      this.itemID = nextId();
      this.url = url;
    }

    public int jobID;
    public String queueID;
    public int itemID;
    public String url;

    @Override
    public int compareTo(Key key) {
      return itemID - key.itemID;
    }
  }

  Key key;
  WebPage page;
  URL u;
  long pendingStartTime = -1;

  public FetchTask(int jobID, String url, WebPage page, URL u, String queueID) {
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

  public int getItemID() {
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

  public long getPendingStartTime() {
    return pendingStartTime;
  }

  public void setPendingStartTime(long pendingStartTime) {
    this.pendingStartTime = pendingStartTime;
  }

  /** 
   * Create an item. Queue id will be created based on <code>queueMode</code>
   * argument, either as a protocol + hostname pair, protocol + IP
   * address pair or protocol+domain pair.
   */
  public static FetchTask create(int jobID, String url, WebPage page, String queueMode) {
    final URL u = getUrl(url);

    if (u == null) {
      return null;
    }

    final String proto = u.getProtocol().toLowerCase();
    final String host = getHost(u, queueMode).toLowerCase();
    final String queueID = proto + "://" + host;

    return new FetchTask(jobID, url, page, u, queueID);
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
  private int nextId() {
    return instanceSequence.incrementAndGet();
  }

  protected static String getHost(URL url, String queueMode) {
    String host;
    if (QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
      try {
        final InetAddress addr = InetAddress.getByName(url.getHost());
        host = addr.getHostAddress();
      } catch (final UnknownHostException e) {
        // unable to resolve it, so don't fall back to host name
        LOG.warn("Unable to resolve: " + url.getHost() + ", skipping.");
        return null;
      }
    }
    else if (QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)){
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
    return "FetchTask [queueID=" + key.queueID + ", id=" + key.itemID + ", url=" + key.url + ", u=" + u
        + ", page=" + page + "]";
  }

  @Override
  public int compareTo(FetchTask fetchTask) {
    return fetchTask == null ? -1 : getKey().compareTo(fetchTask.getKey());
  }
}
