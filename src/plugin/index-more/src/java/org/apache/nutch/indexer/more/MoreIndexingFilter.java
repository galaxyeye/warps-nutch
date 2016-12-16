package org.apache.nutch.indexer.more;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.MimeUtil;
import org.apache.oro.text.regex.*;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;

// import org.apache.solr.common.util.DateUtil;

/**
 * Add (or reset) a few metaData properties as respective fields (if they are
 * available), so that they can be accurately used within the search index.
 * 
 * 'lastModifed' is indexed to support query by date, 'contentLength' obtains
 * content length from the HTTP header, 'type' field is indexed to support query
 * by type and finally the 'title' field is an attempt to reset the title if a
 * content-disposition hint exists. The logic is that such a presence is
 * indicative that the content provider wants the filename therein to be used as
 * the title.
 * 
 * Still need to make content-length searchable!
 * 
 * @author John Xing
 */

public class MoreIndexingFilter implements IndexingFilter {

  /** Get the MimeTypes resolver instance. */
  private MimeUtil MIME;

  private static Collection<GoraWebPage.Field> FIELDS = new HashSet<GoraWebPage.Field>();

  static {
    FIELDS.add(GoraWebPage.Field.HEADERS);
    FIELDS.add(GoraWebPage.Field.CONTENT_TYPE);
    FIELDS.add(GoraWebPage.Field.MODIFIED_TIME);
  }

  @Override
  public IndexDocument filter(IndexDocument doc, String url, WrappedWebPage page) throws IndexingException {
    addTime(doc, page, url);
    addLength(doc, page, url);
    addType(doc, page, url);
    resetTitle(doc, page, url);
    return doc;
  }

  // Add time related meta info. Add last-modified if present. Index date as
  // last-modified, or, if that's not present, use fetch time.
  private IndexDocument addTime(IndexDocument doc, WrappedWebPage page, String url) {
    Instant time = Instant.EPOCH;

    CharSequence lastModified = page.getHeaders().get(new Utf8(HttpHeaders.LAST_MODIFIED));
    if (lastModified != null) { // try parse last-modified
      time = DateTimeUtil.parseTime(lastModified.toString()); // use as time
    }

    if (time.toEpochMilli() > 0) { // if no last-modified
      time = page.getModifiedTime(); // use Modified time
    }

    // un-stored, indexed and un-tokenized
    if (time.toEpochMilli() > 0) {
      doc.add("header_last_modified", DateTimeUtil.solrCompatibleFormat(time));
      doc.add("last_modified_s", DateTimeUtil.solrCompatibleFormat(time));
    }

    return doc;
  }

  // Add Content-Length
  private IndexDocument addLength(IndexDocument doc, WrappedWebPage page, String url) {
    CharSequence contentLength = page.getHeaders().get(new Utf8(HttpHeaders.CONTENT_LENGTH));
    if (contentLength != null) {
      // NUTCH-1010 ContentLength not trimmed
      String trimmed = contentLength.toString().trim();
      if (!trimmed.isEmpty())
        doc.add("content_length", trimmed);
    }

    return doc;
  }

  /**
   * <p>
   * Add Content-Type and its primaryType and subType add contentType,
   * primaryType and subType to field "type" as un-stored, indexed and
   * un-tokenized, so that search results can be confined by contentType or its
   * primaryType or its subType.
   * </p>
   * <p>
   * For example, if contentType is application/vnd.ms-powerpoint, search can be
   * done with one of the following qualifiers
   * type:application/vnd.ms-powerpoint type:application type:vnd.ms-powerpoint
   * all case insensitive. The query filter is implemented in
   * </p>
   * 
   * @param doc
   * @param page
   * @param url
   * @return
   */
  private IndexDocument addType(IndexDocument doc, WrappedWebPage page, String url) {
    String mimeType = null;
    CharSequence contentType = page.getContentType();
    if (contentType == null) {
      contentType = page.getHeaders().get(new Utf8(HttpHeaders.CONTENT_TYPE));
    }
    if (contentType == null) {
      // Note by Jerome Charron on 20050415:
      // Content Type not solved by a previous plugin
      // Or unable to solve it... Trying to find it
      // Should be better to use the doc content too
      // (using MimeTypes.getMimeType(byte[], String), but I don't know
      // which field it is?
      // if (MAGIC) {
      // contentType = MIME.getMimeType(url, content);
      // } else {
      // contentType = MIME.getMimeType(url);
      // }
      mimeType = MIME.getMimeType(url);
    } else {
      mimeType = MIME.forName(MimeUtil.cleanMimeType(contentType.toString()));
    }

    // Checks if we solved the content-type.
    if (mimeType == null) {
      return doc;
    }

    doc.add("mime_type", mimeType);

    // Check if we need to split the content type in sub parts
    if (conf.getBoolean("moreIndexingFilter.indexMimeTypeParts", true)) {
      String[] parts = getParts(mimeType);

      for (String part : parts) {
        doc.add("mime_type", part);
      }
    }

    // leave this for future improvement
    // MimeTypeParameterList parameterList = mimeType.getParameters()

    return doc;
  }

  /**
   * Utility method for splitting mime type into type and subtype.
   * 
   * @param mimeType
   * @return
   */
  static String[] getParts(String mimeType) {
    return mimeType.split("/");
  }

  // Reset title if we see non-standard HTTP header "Content-Disposition".
  // It's a good indication that content provider wants filename therein
  // be used as the title of this url.

  // Patterns used to extract filename from possible non-standard
  // HTTP header "Content-Disposition". Typically it looks like:
  // Content-Disposition: inline; filename="foo.ppt"
  private PatternMatcher matcher = new Perl5Matcher();

  private Configuration conf;
  static Perl5Pattern patterns[] = { null, null };
  static {
    Perl5Compiler compiler = new Perl5Compiler();
    try {
      // order here is important
      patterns[0] = (Perl5Pattern) compiler.compile("\\bfilename=['\"](.+)['\"]");
      patterns[1] = (Perl5Pattern) compiler.compile("\\bfilename=(\\S+)\\b");
    } catch (MalformedPatternException e) {
      // just ignore
    }
  }

  private IndexDocument resetTitle(IndexDocument doc, WrappedWebPage page, String url) {
    CharSequence contentDisposition = page.getHeaders().get(new Utf8(HttpHeaders.CONTENT_DISPOSITION));
    if (contentDisposition == null) {
      return doc;
    }

    MatchResult result;
    for (Perl5Pattern pattern : patterns) {
      if (matcher.contains(contentDisposition.toString(), pattern)) {
        result = matcher.getMatch();
        doc.removeField("meta_title");
        doc.add("meta_title", result.group(1));
        break;
      }
    }

    return doc;
  }

  public void addIndexBackendOptions(Configuration conf) {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    MIME = new MimeUtil(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
