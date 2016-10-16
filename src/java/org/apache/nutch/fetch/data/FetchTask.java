package org.apache.nutch.fetch.data;

import org.apache.nutch.fetch.FetchMonitor;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class described the item to be fetched.
 */
public class FetchTask implements Comparable<FetchTask> {

  private static final Logger LOG = FetchMonitor.LOG;

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
   * Create an item. Queue id will be created based on <code>hostGroupMode</code>
   * argument, either as a protocol + hostname pair, protocol + IP
   * address pair or protocol+domain pair.
   */
  public static FetchTask create(int jobID, String url, WebPage page, URLUtil.HostGroupMode hostGroupMode) {
    final URL u = URLUtil.getUrl(url);

    if (u == null) {
      return null;
    }

    final String proto = u.getProtocol();
    final String host = URLUtil.getHost(u, hostGroupMode);

    if (proto == null || host == null) {
      return null;
    }

    final String queueID = proto.toLowerCase() + "://" + host.toLowerCase();

    return new FetchTask(jobID, url, page, u, queueID);
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
