package org.apache.nutch.fetch.data;

import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.URLUtil;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class described the item to be fetched.
 */
public class FetchTask implements Comparable<FetchTask> {

  /**
   * The initial value is the current timestamp in second, to make it 
   * an unique id in the current host
   * */
  private static AtomicInteger instanceSequence = new AtomicInteger(0);

  public class Key implements Comparable<Key> {

    public Key(int jobID, int priority, String queueId, String url) {
      this.jobID = jobID;
      this.priority = priority;
      this.queueId = queueId;
      this.itemId = nextId();
      this.url = url;
    }

    public int jobID;
    public int priority;
    public String queueId;
    public int itemId;
    public String url;

    @Override
    public int compareTo(Key key) {
      return itemId - key.itemId;
    }
  }

  private Key key;
  private WebPage page;
  private URL u;
  private long pendingStartTime = -1;

  public FetchTask(int jobID, int priority, String url, WebPage page, URL u, String queueId) {
    this.page = page;
    this.key = new Key(jobID, priority, queueId, url);
    this.u = u;
  }

  public WebPage getPage() { return page; }

  public void setPage(WebPage page) { this.page = page; }

  public int getPriority() { return key.priority; }

  public String getQueueId() { return key.queueId; }

  public int getItemId() { return key.itemId; }

  public String getUrl() { return key.url; }

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
  public static FetchTask create(int jobId, int priority, String url, WebPage page, URLUtil.HostGroupMode hostGroupMode) {
    final URL u = URLUtil.getUrl(url);

    if (u == null) {
      return null;
    }

    final String proto = u.getProtocol();
    final String host = URLUtil.getHost(u, hostGroupMode);

    if (proto == null || host == null) {
      return null;
    }

    final String queueId = proto.toLowerCase() + "://" + host.toLowerCase();

    return new FetchTask(jobId, priority, url, page, u, queueId);
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
    return "FetchTask [queueID=" + key.queueId + ", id=" + key.itemId + ", url=" + key.url + ", u=" + u
        + ", page=" + page + "]";
  }

  @Override
  public int compareTo(FetchTask fetchTask) {
    return fetchTask == null ? -1 : getKey().compareTo(fetchTask.getKey());
  }
}
