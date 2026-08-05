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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Verifies {@link RemoteSegmentDirectory} against a real {@code columns.psf} + {@code index_map} written by
 * {@link SingleFileIndexDirectory}, served from a {@code file://} "deep store": byte parity with local reads,
 * the prefetch/acquire/release lifecycle (entries resident only while pinned), load-mode small reads that
 * never make entries resident, and query-mode miss promotion.
 */
public class RemoteSegmentDirectoryTest {
  private static final File DEEP_STORE_ROOT =
      new File(FileUtils.getTempDirectory(), RemoteSegmentDirectoryTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "remoteSeg01";
  private static final String COLUMN = "col1";
  private static final int DICT_SIZE = 64 * 1024;
  private static final int FWD_SIZE = 256 * 1024;

  private File _segmentBaseDir;
  private File _v3Dir;
  private RemoteSegmentMetadata _remoteMetadata;
  private RemoteSegmentDirectory _directory;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(DEEP_STORE_ROOT);
    _segmentBaseDir = new File(DEEP_STORE_ROOT, SEGMENT_NAME);
    _v3Dir = new File(_segmentBaseDir, "v3");
    FileUtils.forceMkdir(_v3Dir);

    // Write two deterministic index entries through the real v3 single-file writer
    try (SingleFileIndexDirectory writer = new SingleFileIndexDirectory(_v3Dir,
        ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v3), ReadMode.mmap)) {
      try (PinotDataBuffer dict = writer.newBuffer(COLUMN, StandardIndexes.dictionary(), DICT_SIZE)) {
        for (int i = 0; i < DICT_SIZE / 4; i++) {
          dict.putInt(i * 4L, i * 31 + 7);
        }
      }
      try (PinotDataBuffer fwd = writer.newBuffer(COLUMN, StandardIndexes.forward(), FWD_SIZE)) {
        for (int i = 0; i < FWD_SIZE / 4; i++) {
          fwd.putInt(i * 4L, i * 17 + 3);
        }
      }
    }

    Map<IndexKey, RemoteSegmentMetadata.IndexRange> ranges =
        RemoteSegmentMetadata.parseIndexMap(new File(_v3Dir, "index_map"), _v3Dir);
    assertEquals(ranges.size(), 2);

    _remoteMetadata = new RemoteSegmentMetadata(SEGMENT_NAME, "12345", _segmentBaseDir.toURI(),
        RemoteSegmentMetadata.columnsPsfUri(_segmentBaseDir.toURI()), ranges,
        ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v3), _segmentBaseDir);
    _directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _directory.close();
    FileUtils.deleteQuietly(DEEP_STORE_ROOT);
  }

  @Test
  public void testParityWithLocalReads()
      throws Exception {
    SegmentDirectory.Reader reader = _directory.createReader();
    assertTrue(reader.hasIndexFor(COLUMN, StandardIndexes.dictionary()));
    assertTrue(reader.hasIndexFor(COLUMN, StandardIndexes.forward()));
    assertFalse(reader.hasIndexFor(COLUMN, StandardIndexes.inverted()));

    PinotDataBuffer remoteDict = reader.getIndexFor(COLUMN, StandardIndexes.dictionary());
    assertEquals(remoteDict.size(), DICT_SIZE);
    // Sampled reads while nothing is resident (load-mode small reads)
    assertEquals(remoteDict.getInt(0), 7);
    assertEquals(remoteDict.getInt(400), 100 * 31 + 7);
    assertEquals(remoteDict.getInt(DICT_SIZE - 4), (DICT_SIZE / 4 - 1) * 31 + 7);

    PinotDataBuffer remoteFwd = reader.getIndexFor(COLUMN, StandardIndexes.forward());
    assertEquals(remoteFwd.size(), FWD_SIZE);
    for (int i : new int[]{0, 1, 999, FWD_SIZE / 4 - 1}) {
      assertEquals(remoteFwd.getInt(i * 4L), i * 17 + 3, "mismatch at value " + i);
    }

    // Bulk copy parity across a block of values
    byte[] remoteBytes = new byte[4096];
    remoteFwd.copyTo(8192, remoteBytes, 0, 4096);
    for (int i = 0; i < 1024; i++) {
      int expected = (2048 + i) * 17 + 3;
      int actual = ((remoteBytes[i * 4] & 0xFF) << 24) | ((remoteBytes[i * 4 + 1] & 0xFF) << 16)
          | ((remoteBytes[i * 4 + 2] & 0xFF) << 8) | (remoteBytes[i * 4 + 3] & 0xFF);
      assertEquals(actual, expected, "bulk mismatch at value " + i);
    }
    reader.close();
  }

  @Test
  public void testPrefetchAcquireReleaseLifecycle()
      throws Exception {
    IndexKey dictKey = new IndexKey(COLUMN, StandardIndexes.dictionary());
    FetchContext fetchContext = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
        Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));

    assertNull(_directory.peekResident(dictKey));
    _directory.prefetch(fetchContext);
    _directory.acquire(fetchContext);
    assertNotNull(_directory.peekResident(dictKey), "entry must be resident after acquire");
    assertTrue(_directory.inQueryMode());

    // Reads while resident are served from memory
    SegmentDirectory.Reader reader = _directory.createReader();
    PinotDataBuffer dict = reader.getIndexFor(COLUMN, StandardIndexes.dictionary());
    assertEquals(dict.getInt(0), 7);

    _directory.release(fetchContext);
    assertFalse(_directory.inQueryMode());
    assertNull(_directory.peekResident(dictKey), "entry must be freed after release");
  }

  @Test
  public void testQueryModeMissPromotesWholeEntry()
      throws Exception {
    IndexKey fwdKey = new IndexKey(COLUMN, StandardIndexes.forward());
    // Acquire a context that pins only the dictionary; the forward index read below is a query-mode miss
    FetchContext fetchContext = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
        Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));
    _directory.acquire(fetchContext);
    int missesBefore = _directory.getSyncMisses();
    try {
      SegmentDirectory.Reader reader = _directory.createReader();
      PinotDataBuffer fwd = reader.getIndexFor(COLUMN, StandardIndexes.forward());
      // Force a fresh buffer (query mode): large read promotes the whole entry
      byte[] chunk = new byte[FWD_SIZE];
      fwd.copyTo(0, chunk, 0, FWD_SIZE);
      assertTrue(_directory.getSyncMisses() > missesBefore, "query-mode miss must promote");
      assertNotNull(_directory.peekResident(fwdKey));
    } finally {
      _directory.release(fetchContext);
    }
    // Stray promoted by the miss is freed once no query is active
    assertNull(_directory.peekResident(fwdKey), "stray must be freed when queries drain");
  }

  @Test
  public void testWriterUnsupported() {
    assertThrows(UnsupportedOperationException.class, () -> _directory.createWriter());
  }

  @Test
  public void testMissingIndexThrows()
      throws Exception {
    SegmentDirectory.Reader reader = _directory.createReader();
    assertThrows(IllegalStateException.class, () -> reader.getIndexFor("nope", StandardIndexes.forward()));
  }

  @Test
  public void testDoubleReleaseDoesNotUnderflow()
      throws Exception {
    // Upstream releases the same fetch context twice (combine operator + response operator)
    IndexKey dictKey = new IndexKey(COLUMN, StandardIndexes.dictionary());
    FetchContext first = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
        Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));
    _directory.acquire(first);
    _directory.release(first);
    _directory.release(first);
    assertFalse(_directory.inQueryMode());

    // A later query still gets a correct lifecycle after the double release
    FetchContext second = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
        Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));
    _directory.acquire(second);
    assertTrue(_directory.inQueryMode(), "second query must still enter query mode");
    assertNotNull(_directory.peekResident(dictKey));
    _directory.release(second);
    assertFalse(_directory.inQueryMode());
    assertNull(_directory.peekResident(dictKey), "entry must be freed after the second query releases");
  }

  @Test
  public void testPrefetchOnlyReleaseWithoutAcquire() {
    // Bloom-pruner pattern: prefetch + release with no acquire in between
    IndexKey dictKey = new IndexKey(COLUMN, StandardIndexes.dictionary());
    FetchContext fetchContext = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
        Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));
    _directory.prefetch(fetchContext);
    _directory.release(fetchContext);
    assertFalse(_directory.inQueryMode());
    assertNull(_directory.peekResident(dictKey), "pins must drain on prefetch-only release");
  }
}
