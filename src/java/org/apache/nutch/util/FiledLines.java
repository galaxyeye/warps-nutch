package org.apache.nutch.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.io.Files;

public class FiledLines {

  private static final Logger logger = LoggerFactory.getLogger(FiledLines.class);

  private Comparator<String> wordsComparator = null;

  private Map<String, TreeMultiset<String>> filedLines = new HashMap<String, TreeMultiset<String>>();

  private Preprocessor preprocessor = new DefaultPreprocessor();

  public interface Preprocessor {
    public String process(String line);
  }

  public class DefaultPreprocessor implements Preprocessor {
    @Override
    public String process(String line) {
      return line.startsWith("#") ? "" : line.trim();
    }
  };

  public FiledLines() {
  }

  public FiledLines(String... files) {
    Validate.notNull(files);
    try {
      load(files);
    } catch (IOException e) {
      logger.error("{}, files : {}", e, Arrays.asList(files));
    }
  }

  public FiledLines(Comparator<String> wordsComparator, String... files) {
    this(files);
    this.wordsComparator = wordsComparator;
  }

  public Preprocessor getPreprocessor() {
    return preprocessor;
  }

  public void setPreprocessor(Preprocessor preprocessor) {
    this.preprocessor = preprocessor;
  }

  public Multiset<String> getLines(String file) {
    if (filedLines.isEmpty()) return TreeMultiset.create();

    return filedLines.get(file);
  }

  public Multiset<String> firstFileLines() {
    if (filedLines.isEmpty()) return TreeMultiset.create();

    return filedLines.values().iterator().next();
  }

  public boolean add(String file, String text) {
    Multiset<String> lines = getLines(file);

    if (lines == null) {
      return false;
    }

    return lines.add(text);
  }

  public boolean addAll(String file, Collection<String> texts) {
    Multiset<String> lines = getLines(file);

    if (lines == null) {
      return false;
    }

    return lines.addAll(texts);
  }

  public boolean remove(String file, String text) {
    Multiset<String> lines = getLines(file);

    if (lines == null) {
      return false;
    }

    return lines.remove(text);
  }

  public void clear() {
    filedLines.clear();
  }

  public boolean contains(String file, String text) {
    Multiset<String> conf = getLines(file);
    if (conf != null)
      return conf.contains(text);

    return false;
  }

  public void load(String... files) throws IOException {
    if (files.length == 0) {
      logger.error("no file to load");
    }

    for (String file : files) {
      if (file != null && file.length() > 0) {
        TreeMultiset<String> values = TreeMultiset.create(wordsComparator);
        List<String> lines = Files.readLines(new File(file), Charsets.UTF_8);

        for (String line : lines) {
          line = preprocessor.process(line);
          if (line != null && !line.isEmpty()) values.add(line);
        }

        filedLines.put(file, values);
      }
      else {
        logger.error("bad file name");
      }
    }
  }

  public void save(String file) throws IOException {
    PrintWriter pw = new PrintWriter(new FileWriter(file));

    for (String line : filedLines.get(file).elementSet()) {
      pw.println(line);
    }

    pw.close();
  }

  public void saveAll() throws IOException {
    for (String file : filedLines.keySet()) {
      save(file);
    }
  }
}
