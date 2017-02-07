package org.apache.nutch.parse.tika;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by vincent on 17-1-26.
 */
public class TikaTestBase {
    protected String fileSeparator = System.getProperty("file.separator");
    // This system property is defined in ./src/plugin/build-plugin.xml
    protected String sampleDir = System.getProperty("test.data", ".");

    protected Configuration conf = ConfigUtils.create();
    protected MimeUtil mimeutil = new MimeUtil(conf);

    public WebPage parse(String sampleFile) throws IOException {
        String baseUrl = "file:" + sampleDir + fileSeparator + sampleFile;

        Path path = Paths.get(sampleDir, sampleFile);
        byte[] bytes = Files.readAllBytes(path);
        NutchUtil.LOG.debug(baseUrl);

        WebPage page = WebPage.newWebPage();
        page.setBaseUrl(new Utf8(baseUrl));
        page.setContent(bytes);
        String mtype = mimeutil.getMimeType(baseUrl);
        page.setContentType(mtype);

        TikaParser parser = new TikaParser(conf);
        parser.getParse(baseUrl, page);

        return page;
    }
}
