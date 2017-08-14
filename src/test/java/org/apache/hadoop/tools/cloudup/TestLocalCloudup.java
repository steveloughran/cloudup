/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.cloudup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.*;

public class TestLocalCloudup extends Assert {

  @Rule
  public TestName methodName = new TestName();

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(15 * 1000);

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestLocalCloudup.class);
  private static File testDirectory;
  private File methodDir;
  private File sourceDir;
  private File destDir;

  @BeforeClass
  public static void classSetup() throws Exception {
    Thread.currentThread().setName("JUnit");

    String testDir = System.getProperty("test.build.data");
    if (testDir == null) {
      File tf = File.createTempFile("TestLocalCloudup", ".dir");
      tf.delete();
      testDir = tf.getAbsolutePath();
      testDirectory = new File(testDir);
    } else {
      testDirectory = new File(testDir);
      // test dir from sysprop; force delete
      FileUtil.fullyDelete(testDirectory);
    }
    mkdirs(testDirectory);
  }


  protected static void mkdirs(File dir) {
    assertTrue("Failed to create " + dir, dir.mkdirs());
  }

  @Before
  public void setup() throws Exception {
    methodDir = new File(testDirectory, methodName.getMethodName());
    mkdirs(methodDir);
    sourceDir = new File(methodDir, "src");
    destDir = new File(methodDir, "dest");
  }

  @After
  public void teardown() throws Exception {
    if (methodDir != null) {
      FileUtil.fullyDelete(methodDir);
    }

  }


  @Test
  public void testNoArgs() throws Throwable {
    // no args == failure
    expectException(IllegalArgumentException.class, new String[0]);
  }

  @Test
  public void testNonexistentSrcAndDest() throws Throwable {
    expectException(FileNotFoundException.class,
        "-s", sourceDir.toURI().toString(),
        "-d", destDir.toURI().toString());
  }

  @Test
  public void testCopyFileSrcAndDest() throws Throwable {
    FileUtils.write(sourceDir, "hello");
    expectSuccess(
        "-s", sourceDir.toURI().toString(),
        "-d", destDir.toURI().toString());
    assertTrue(destDir.isFile());
  }

  @Test
  public void testCopyRecursive() throws Throwable {
    File subdir = new File(sourceDir, "subdir");
    int expected = 0;
    mkdirs(subdir);
    File top = new File(sourceDir, "top");
    FileUtils.write(top, "toplevel");
    expected++;
    int size = 32;
    for (int i = 0; i < size; i++) {
      String text = String.format("file-%02d", i);
      File f = new File(subdir, text);
      FileUtils.write(f, f.toString());
    }
    expected += size;

    // and write the largest file
    File largest = new File(subdir, "largest");
    FileUtils.writeByteArrayToFile(largest,
        ContractTestUtils.dataset(8192, 32, 64));
    expected++;

    expectSuccess(
        "-s", sourceDir.toURI().toString(),
        "-d", destDir.toURI().toString(),
        "-t", "4",
        "-l", "3");

    LocalFileSystem local = FileSystem.getLocal(new Configuration());
    Set<String> entries = new TreeSet<>();
    RemoteIterator<LocatedFileStatus> iterator
        = local.listFiles(new Path(destDir.toURI()), true);
    int count = 0;
    while (iterator.hasNext()) {
      LocatedFileStatus next = iterator.next();
      entries.add(next.getPath().toUri().toString());
      LOG.info("Entry {} size = {}", next.getPath(), next.getLen());
      count++;
    }
    assertEquals("Mismatch in files found", expected, count);





  }

  private int exec(String... args) throws Exception {
    return Cloudup.exec(args);
  }

  private void expectSuccess(String... args) throws Exception {
    expectOutcome(0, args);
  }

  private <E extends Throwable> E expectException(Class<E> clazz,
      final String... args) throws Exception {
    return intercept(clazz,
        new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return exec(args);
          }
        });
  }

  private void expectOutcome(int expected, String... args) throws Exception {
    assertEquals(toString(args), expected, exec(args));
  }

  private String toString(String[] args) {
    return "exec(" + StringUtils.join(args, " ") + ")";
  }

}
