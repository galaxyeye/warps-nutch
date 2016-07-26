package org.apache.nutch.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.service.model.request.SeedUrl;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Created by vincent on 16-7-20.
 */
public class TestAnything {

  @Test
  public void testReadLines() throws IOException {
    String file = "conf/urls/information.txt";

    List<String> lines = Files.readAllLines(Paths.get(file));
    Set<String> lines2 = Sets.newHashSet();

    lines.stream().forEach(url -> {
      String pattern = StringUtils.substringBetween(url, "://", "/");
      pattern = "+http://" + pattern + "/(.+)";
      lines2.add(pattern);
    });

    Files.write(Paths.get("/tmp/1"), StringUtils.join(lines2, "\n").getBytes());

    System.out.println(lines2.size());
    System.out.println(StringUtils.join(lines2));
  }

  @Test
  public void testUniqueSeedUrls() {
    List<SeedUrl> seedUrls = Lists.newArrayList();
    for (int i = 0; i < 10; i += 2) {
      seedUrls.add(new SeedUrl(i + 0L, "http://www.warpspeed.cn/" + i));
      seedUrls.add(new SeedUrl(i + 1L, "http://www.warpspeed.cn/" + i));
    }
    Set<SeedUrl> uniqueSeedUrls = Sets.newTreeSet(new Comparator<SeedUrl>() {
      @Override
      public int compare(SeedUrl seedUrl, SeedUrl seedUrl2) {
        return seedUrl.getUrl().compareTo(seedUrl2.getUrl());
      }
    });
    uniqueSeedUrls.addAll(seedUrls);
    uniqueSeedUrls.stream().forEach(seedUrl -> {
      System.out.println(seedUrl.getUrl());
    });

  }
}
