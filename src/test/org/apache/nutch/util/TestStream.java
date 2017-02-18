package org.apache.nutch.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by vincent on 16-7-20.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class TestStream {

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

    // longer url comes first
    urls.stream().sorted((u1, u2) -> u2.length() - u1.length()).forEach(System.out::println);

    urls.stream().map(URLUtil::getHostName).filter(Objects::nonNull).forEach(System.out::println);

    for (String url : urls) {
      URL u = new URL(url);
      System.out.println(u.hashCode() + ", " + url.hashCode());
    }
  }
}
