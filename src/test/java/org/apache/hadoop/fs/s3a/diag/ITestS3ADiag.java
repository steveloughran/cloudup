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

package org.apache.hadoop.fs.s3a.diag;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.cloudup.AbstractS3ACloudupTest;

/**
 * As the S3A test base isn't available, do enough to make it look
 * like it is, to ease later merge.
 */
public class ITestS3ADiag extends AbstractS3ACloudupTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ADiag.class);

  @Test
  public void testExec() throws Throwable {
    S3ADiag.exec(getFileSystem().getUri().toString());
  }

  @Override
  protected void deleteTestDirInTeardown() {

  }
}
