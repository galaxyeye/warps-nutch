package org.apache.nutch.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.service.model.request.SeedUrl;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by vincent on 16-7-20.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class TestAnything {

  @Test
  public void generateRegexUrlFilter() throws IOException {
    String[] files = {
        "conf/seeds/aboard.txt",
        "conf/seeds/bbs.txt",
        "conf/seeds/national.txt",
        "conf/seeds/papers.txt"
    };

    List<String> lines = Lists.newArrayList();
    for (String file : files) {
      lines.addAll(Files.readAllLines(Paths.get(file)));
    }

    Set<String> lines2 = Sets.newHashSet();
    lines.stream().forEach(url -> {
      String pattern = StringUtils.substringBetween(url, "://", "/");
      pattern = "+http://" + pattern + "/(.+)";
      lines2.add(pattern);
    });

    Files.write(Paths.get("/tmp/regex-urlfilter.txt"), StringUtils.join(lines2, "\n").getBytes());

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

  @Test
  public void testSystem() {
    String username = System.getenv("USER");
    System.out.println(username);
  }

  @Test
  public void normalizeUrlLists() throws IOException {
    String filename = "/home/vincent/Tmp/novel-list.txt";
    List<String> lines = Files.readAllLines(Paths.get(filename));
    Set<String> urls = Sets.newHashSet();
    Set<String> domains = Sets.newHashSet();
    Set<String> regexes = Sets.newHashSet();

    lines.stream().forEach(url -> {
      int pos = StringUtils.indexOfAny(url, "abcdefjhijklmnopqrstufwxyz");
      if (pos >= 0) {
        url = url.substring(pos);
        urls.add("http://" + url);
        domains.add(url);
        regexes.add("+http://www." + url + "(.+)");
      }
    });

    Files.write(Paths.get("/tmp/domain-urlfilter.txt"), StringUtils.join(domains, "\n").getBytes());
    Files.write(Paths.get("/tmp/novel.seeds.txt"), StringUtils.join(urls, "\n").getBytes());
    Files.write(Paths.get("/tmp/regex-urlfilter.txt"), StringUtils.join(regexes, "\n").getBytes());

    System.out.println(urls.size());
    System.out.println(StringUtils.join(urls, ","));
  }

  @Test
  public void testTreeMap() {
    final Map<Integer, String> ints = new TreeMap<>(Comparator.reverseOrder());
    ints.put(1, "1");
    ints.put(2, "2");
    ints.put(3, "3");
    ints.put(4, "4");
    ints.put(5, "5");

    System.out.println(ints.keySet().iterator().next());
  }

  @Test
  public void testTime() {
    System.out.println(Duration.ofMinutes(60).toMillis());
  }

  @Test
  public void testAtomic() {
    AtomicInteger counter = new AtomicInteger(100);
    int deleted = 10;
    counter.addAndGet(0 - deleted);
    System.out.println(counter);
  }
}
