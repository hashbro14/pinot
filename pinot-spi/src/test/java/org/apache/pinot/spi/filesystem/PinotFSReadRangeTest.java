/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/**
 * Tests {@link PinotFS#readRange} for both the {@link LocalPinotFS} override and the interface default
 * implementation (exercised through a minimal file system that only provides {@link PinotFS#open}).
 */
public class PinotFSReadRangeTest {
  private static final int FILE_SIZE = 10_000;

  private File _tempDir;
  private File _dataFile;
  private byte[] _expected;

  @BeforeClass
  public void setUp()
      throws IOException {
    _tempDir = new File(System.getProperty("java.io.tmpdir"), PinotFSReadRangeTest.class.getSimpleName());
    FileUtils.deleteQuietly(_tempDir);
    FileUtils.forceMkdir(_tempDir);
    _dataFile = new File(_tempDir, "data.bin");
    _expected = new byte[FILE_SIZE];
    for (int i = 0; i < FILE_SIZE; i++) {
      _expected[i] = (byte) (i * 31 + 7);
    }
    Files.write(_dataFile.toPath(), _expected);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @DataProvider(name = "fileSystems")
  public Object[][] fileSystems() {
    return new Object[][]{{new LocalPinotFS()}, {new OpenOnlyPinotFS()}};
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeMidFile(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[100];
    int read = fs.readRange(_dataFile.toURI(), 1234, buffer, 0, 100);
    assertEquals(read, 100);
    for (int i = 0; i < 100; i++) {
      assertEquals(buffer[i], _expected[1234 + i], "mismatch at " + i);
    }
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeFromStart(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[64];
    int read = fs.readRange(_dataFile.toURI(), 0, buffer, 0, 64);
    assertEquals(read, 64);
    for (int i = 0; i < 64; i++) {
      assertEquals(buffer[i], _expected[i], "mismatch at " + i);
    }
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeWithBufferOffset(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[100];
    int read = fs.readRange(_dataFile.toURI(), 500, buffer, 40, 60);
    assertEquals(read, 60);
    for (int i = 0; i < 60; i++) {
      assertEquals(buffer[40 + i], _expected[500 + i], "mismatch at " + i);
    }
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeTruncatedAtEof(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[100];
    int read = fs.readRange(_dataFile.toURI(), FILE_SIZE - 25, buffer, 0, 100);
    assertEquals(read, 25);
    for (int i = 0; i < 25; i++) {
      assertEquals(buffer[i], _expected[FILE_SIZE - 25 + i], "mismatch at " + i);
    }
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeAtEof(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[10];
    assertEquals(fs.readRange(_dataFile.toURI(), FILE_SIZE, buffer, 0, 10), -1);
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeBeyondEof(PinotFS fs)
      throws IOException {
    byte[] buffer = new byte[10];
    assertEquals(fs.readRange(_dataFile.toURI(), FILE_SIZE + 1000, buffer, 0, 10), -1);
  }

  @Test(dataProvider = "fileSystems")
  public void testReadRangeIllegalArguments(PinotFS fs) {
    byte[] buffer = new byte[10];
    assertThrows(IllegalArgumentException.class, () -> fs.readRange(_dataFile.toURI(), -1, buffer, 0, 5));
    assertThrows(IllegalArgumentException.class, () -> fs.readRange(_dataFile.toURI(), 0, buffer, 0, -1));
    assertThrows(IllegalArgumentException.class, () -> fs.readRange(_dataFile.toURI(), 0, buffer, -1, 5));
    assertThrows(IllegalArgumentException.class, () -> fs.readRange(_dataFile.toURI(), 0, buffer, 5, 10));
  }

  /**
   * Minimal file system that only implements {@link #open}, so that {@code readRange} runs the interface default
   * (open + skip) implementation.
   */
  private static class OpenOnlyPinotFS implements PinotFS {
    @Override
    public InputStream open(URI uri)
        throws IOException {
      return new FileInputStream(new File(uri));
    }

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public boolean mkdir(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteBatch(List<URI> segmentUris, boolean forceDelete) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean move(URI srcUri, URI dstUri, boolean overwrite) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists(URI fileUri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long length(URI fileUri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirectory(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long lastModified(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean touch(URI uri) {
      throw new UnsupportedOperationException();
    }
  }
}
