package org.apache.nutch.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.Set;

/**
 * Created by vincent on 16-7-20.
 */
public class TestFilesystem {

  @Test
  public void testFiles() throws IOException {
    String readableTime = new SimpleDateFormat("MMdd.HHmmss").format(System.currentTimeMillis());

    Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwx------");

    Path commandFile = Paths.get("/tmp/nutch/command/test." + readableTime + ".sh");
    Files.createFile(commandFile, PosixFilePermissions.asFileAttribute(permissions));
    Files.write(commandFile, "#bin\necho hello world".getBytes());
  }

  @Test
  public void testCreateFiles() throws IOException {
    String readableTime = new SimpleDateFormat("MMdd.HHmmss").format(System.currentTimeMillis());

    Path cmdFile = Paths.get("/tmp/nutch/command/test." + readableTime + ".sh");
    Files.write(cmdFile, "#bin\necho hello world".getBytes(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    Files.setPosixFilePermissions(cmdFile, PosixFilePermissions.fromString("rwxrw-r--"));
  }
}
