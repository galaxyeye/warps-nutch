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
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nutch.metadata.Nutch.PARAM_FETCH_MAX_INTERVAL;
import static org.junit.Assert.assertEquals;

/**
 * Created by vincent on 16-7-20.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class TestAnything {

  private Configuration conf = ConfigUtils.create();

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
  public void testTreeSet() {
    TreeSet<Integer> integers = IntStream.range(0, 30).boxed().collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
    integers.forEach(System.out::print);
    integers.pollLast();
    System.out.println();
    integers.forEach(System.out::print);
  }

  @Test
  public void testOrderedStream() {
    int[] counter = {0, 0};

    TreeSet<Integer> orderedIntegers = IntStream.range(0, 1000000).boxed().collect(TreeSet::new, TreeSet::add, TreeSet::addAll);
    long startTime = System.currentTimeMillis();
    int result = orderedIntegers.stream().filter(i -> {
      counter[0]++;
      return i < 1000;
    }).map(i -> i * 2).reduce(0, (x, y) -> x + 2 * y);
    long endTime = System.currentTimeMillis();
    System.out.println("Compute over ordered integers, time elapsed : " + (endTime - startTime) / 1000.0 + "s");
    System.out.println("Result : " + result);

    startTime = System.currentTimeMillis();
    result = 0;
    int a = 0;
    int b = 0;
    for (Integer i : orderedIntegers) {
      if (i < 1000) {
        b = i;
        b *= 2;
        result += a + 2 * b;
      } else {
        break;
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Compute over ordered integers, handy code, time elapsed : " + (endTime - startTime) / 1000.0 + "s");
    System.out.println("Result : " + result);

    List<Integer> unorderedIntegers = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
    startTime = System.currentTimeMillis();
    result = unorderedIntegers.stream().filter(i -> {
      counter[1]++;
      return i < 1000;
    }).map(i -> i * 2).reduce(0, (x, y) -> x + 2 * y);
    endTime = System.currentTimeMillis();
    System.out.println("Compute over unordered integers, time elapsed : " + (endTime - startTime) / 1000.0 + "s");
    System.out.println("Result : " + result);

    System.out.println("Filter loops : " + counter[0] + ", " + counter[1]);
  }

  @Test
  public void testURL() throws MalformedURLException {
    List<String> urls = Lists.newArrayList(
        "http://bond.eastmoney.com/news/1326,20160811671616734.html",
        "http://bond.eastmoney.com/news/1326,20161011671616734.html",
        "http://tech.huanqiu.com/photo/2016-09/2847279.html",
        "http://tech.hexun.com/2016-09-12/186368492.html",
        "http://opinion.cntv.cn/2016/04/17/ARTI1397735301366288.shtml",
        "http://tech.hexun.com/2016-11-12/186368492.html",
        "http://ac.cheaa.com/2016/0905/488888.shtml",
        "http://ankang.hsw.cn/system/2016/0927/16538.shtml",
        "http://auto.nbd.com.cn/articles/2016-09-28/1042037.html",
        "http://bank.cnfol.com/pinglunfenxi/20160901/23399283.shtml",
        "http://bank.cnfol.com/yinhanglicai/20160905/23418323.shtml"
    );

    for (String url : urls) {
      URL u = new URL(url);
      System.out.println(u.hashCode() + ", " + url.hashCode());
    }
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
