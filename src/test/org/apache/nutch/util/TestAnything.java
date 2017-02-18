package org.apache.nutch.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.filter.PageCategory;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.client.NutchClient;
import org.apache.nutch.persist.local.model.ServerInstance;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nutch.metadata.Nutch.PARAM_FETCH_MAX_INTERVAL;
import static org.junit.Assert.assertEquals;

/**
 * Created by vincent on 16-7-20.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class TestAnything {

  private Configuration conf = ConfigUtils.create();

  @Test
  @Ignore
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
    lines.forEach(url -> {
      String pattern = StringUtils.substringBetween(url, "://", "/");
      pattern = "+http://" + pattern + "/(.+)";
      lines2.add(pattern);
    });

    Files.write(Paths.get("/tmp/regex-urlfilter.txt"), StringUtils.join(lines2, "\n").getBytes());

    System.out.println(lines2.size());
    System.out.println(StringUtils.join(lines2, ","));
  }

  @Test
  public void testSystem() {
    String username = System.getenv("USER");
    System.out.println(username);
  }

  @Test
  @Ignore
  public void normalizeUrlLists() throws IOException {
    String filename = "/home/vincent/Tmp/novel-list.txt";
    List<String> lines = Files.readAllLines(Paths.get(filename));
    Set<String> urls = Sets.newHashSet();
    Set<String> domains = Sets.newHashSet();
    Set<String> regexes = Sets.newHashSet();

    lines.forEach(url -> {
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
  public void testNutchClient() {
    conf.set(Nutch.PARAM_NUTCH_MASTER_HOST, "localhost");
    NutchClient client = new NutchClient(conf);
    String result = client.test(ServerInstance.Type.FetcherServer, 21001);
    client.recyclePort(ServerInstance.Type.FetcherServer, 21001);
    System.out.println(result);
  }

  @Test
  public void testEnum() {
    PageCategory pageCategory;
    try {
      pageCategory = PageCategory.valueOf("APP");
    } catch (Throwable e) {
      System.out.println(e.getLocalizedMessage());
      pageCategory = PageCategory.UNKNOWN;
    }

    assertEquals(pageCategory, PageCategory.UNKNOWN);
  }

  @Test
  public void testUtf8() {
    String s = "";
    Utf8 u = new Utf8(s);
    assertEquals(0, u.length());
//    System.out.println(u.length());
//    System.out.println(u.toString());
  }

  @Test
  public void testAtomic() {
    AtomicInteger counter = new AtomicInteger(100);
    int deleted = 10;
    counter.addAndGet(-deleted);
    System.out.println(counter);
  }

  @Test
  public void testConfig() throws IOException {
    Configuration conf = ConfigUtils.create();
    Duration maxInterval = ConfigUtils.getDuration(conf, PARAM_FETCH_MAX_INTERVAL, Duration.ofDays(90));
    System.out.println(maxInterval);

    Path path = Paths.get("/home/vincent/workspace/warps-nutch/build/urlfilter-domain/test/data/hosts.txt");
    System.out.println(Files.readAllLines(path));

    Reader reader = conf.getConfResourceAsReader("hosts.txt");
    String line;
    BufferedReader bufferedReader = new BufferedReader(reader);
    while ((line = bufferedReader.readLine()) != null) {
      System.out.println(line);
    }
  }
}
