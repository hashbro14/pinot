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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
  private static final String STRING_COLUMN = "strCol";
  private static final int NUM_STRINGS = 20000;
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
      byte[] varLengthDict = buildVarLengthDictionary();
      try (PinotDataBuffer strDict = writer.newBuffer(STRING_COLUMN, StandardIndexes.dictionary(),
          varLengthDict.length)) {
        strDict.readFrom(0, varLengthDict, 0, varLengthDict.length);
      }
      try (PinotDataBuffer fwd = writer.newBuffer(COLUMN, StandardIndexes.forward(), FWD_SIZE)) {
        for (int i = 0; i < FWD_SIZE / 4; i++) {
          fwd.putInt(i * 4L, i * 17 + 3);
        }
      }
    }

    Map<IndexKey, RemoteSegmentMetadata.IndexRange> ranges =
        RemoteSegmentMetadata.parseIndexMap(new File(_v3Dir, "index_map"), _v3Dir);
    assertEquals(ranges.size(), 3);

    _remoteMetadata = new RemoteSegmentMetadata(SEGMENT_NAME, "12345", _segmentBaseDir.toURI(),
        RemoteSegmentMetadata.columnsPsfUri(_segmentBaseDir.toURI()), ranges,
        ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v3), _segmentBaseDir, null);
    // Eager prefetching is off in production but is what the acquire/release lifecycle tests exercise
    _directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30),
        RemoteQueryConfigs.DEFAULT_MAX_FETCH_BYTES_PER_QUERY, RemoteQueryConfigs.promoteMaxBytes(), true);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _directory.close();
    FileUtils.deleteQuietly(DEEP_STORE_ROOT);
  }

  /** Serializes a var-length string dictionary the same way segment generation does. */
  private static byte[] buildVarLengthDictionary()
      throws Exception {
    File tmp = new File(DEEP_STORE_ROOT, "varLengthDict.tmp");
    try (VarLengthValueWriter writer = new VarLengthValueWriter(tmp, NUM_STRINGS)) {
      for (int i = 0; i < NUM_STRINGS; i++) {
        writer.add(expectedString(i).getBytes(StandardCharsets.UTF_8));
      }
    }
    byte[] bytes = FileUtils.readFileToByteArray(tmp);
    FileUtils.deleteQuietly(tmp);
    return bytes;
  }

  /** Dictionaries are sorted, so pad to a fixed width to keep dictId order == value order. */
  private static String expectedString(int i) {
    return String.format("value-%08d", i);
  }

  /**
   * A projection resolves a whole block of dictIds at once. Over remote storage that batch has to turn
   * into a handful of coalesced ranged reads, not one read per value — while still returning exactly
   * what the local dictionary would.
   */
  @Test
  public void testBatchedDictionaryReadsAreCoalesced()
      throws Exception {
    // A promote ceiling below the dictionary size is what production hits with a multi-GB dictionary:
    // the entry cannot be pulled whole, so the batch has to be coalesced instead.
    try (RemoteSegmentDirectory directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30), 1L << 30, 4096, true);
        SegmentDirectory.Reader reader = directory.createReader()) {
      RemoteSegmentBuffer buffer =
          (RemoteSegmentBuffer) reader.getIndexFor(STRING_COLUMN, StandardIndexes.dictionary());
      StringDictionary dictionary =
          new StringDictionary(buffer, NUM_STRINGS, expectedString(0).length());

      // 1000 dictIds scattered across the dictionary, in the arbitrary order a projection produces
      int[] dictIds = new int[1000];
      for (int i = 0; i < dictIds.length; i++) {
        dictIds[i] = (i * 7919) % NUM_STRINGS;
      }
      String[] values = new String[dictIds.length];
      dictionary.readStringValues(dictIds, dictIds.length, values);

      for (int i = 0; i < dictIds.length; i++) {
        assertEquals(values[i], expectedString(dictIds[i]), "wrong value for dictId " + dictIds[i]);
      }
      long fetches = buffer.getRangeFetchCount();
      assertTrue(fetches < 100,
          "batched dictionary read should coalesce into few ranged reads, issued " + fetches);
    }
  }

  /**
   * A batch too scattered to coalesce must not turn into one request per value.
   *
   * <p>Bounding only the bytes is not enough: values spread thinly across a large entry stay well under
   * any byte ceiling while still needing a round trip each, and a query issues one batch per block, per
   * column, per segment. That is what turned a LIMIT 4000 into 19,277 GETs and a timeout.
   */
  @Test
  public void testScatteredBatchDoesNotExplodeIntoOneRequestPerValue()
      throws Exception {
    try (RemoteSegmentDirectory directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30), 1L << 30, 4096, true);
        SegmentDirectory.Reader reader = directory.createReader()) {
      RemoteSegmentBuffer buffer =
          (RemoteSegmentBuffer) reader.getIndexFor(STRING_COLUMN, StandardIndexes.dictionary());
      StringDictionary dictionary =
          new StringDictionary(buffer, NUM_STRINGS, expectedString(0).length());

      // Maximally scattered: every 20th value across the whole dictionary, so nothing merges at the
      // default gap and the naive path would issue one request per value.
      int[] dictIds = new int[NUM_STRINGS / 20];
      for (int i = 0; i < dictIds.length; i++) {
        dictIds[i] = i * 20;
      }
      String[] values = new String[dictIds.length];
      dictionary.readStringValues(dictIds, dictIds.length, values);

      for (int i = 0; i < dictIds.length; i++) {
        assertEquals(values[i], expectedString(dictIds[i]), "wrong value for dictId " + dictIds[i]);
      }
      long fetches = buffer.getRangeFetchCount();
      assertTrue(fetches < dictIds.length / 4,
          "a scattered batch of " + dictIds.length + " values must not cost a request each, issued " + fetches);
    }
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
    // acquire does not block: the entry is in flight or resident, and the first read waits for it
    assertNotNull(_directory.awaitResident(dictKey), "entry must be resident once awaited after acquire");
    assertNotNull(_directory.peekResident(dictKey));
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
    assertNotNull(_directory.awaitResident(dictKey));
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

  @Test
  public void testDimensionTableCannotBeRemote() {
    // Dimension tables are replicated to every server for lookup joins and must be read locally
    TableConfig dimTable = new TableConfigBuilder(TableType.OFFLINE).setTableName("dim").setIsDimTable(true)
        .setCustomConfig(new TableCustomConfig(
            Map.of(RemoteQueryConfigs.TABLE_REMOTE_QUERY_ENABLED, "true",
                RemoteQueryConfigs.TABLE_REMOTE_QUERY_BASE_URI, "s3://bucket/dim/")))
        .build();
    assertThrows(IllegalStateException.class, () -> RemoteQueryConfigs.fromTableConfig(dimTable));
  }

  @Test
  public void testRemoteTableRequiresBaseUri() {
    TableConfig noUri = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setCustomConfig(new TableCustomConfig(Map.of(RemoteQueryConfigs.TABLE_REMOTE_QUERY_ENABLED, "true")))
        .build();
    assertThrows(IllegalStateException.class, () -> RemoteQueryConfigs.fromTableConfig(noUri));
  }

  @Test
  public void testNonRemoteTableIsIgnored() {
    TableConfig plain = new TableConfigBuilder(TableType.OFFLINE).setTableName("plain").build();
    assertNull(RemoteQueryConfigs.fromTableConfig(plain), "plain tables must not be treated as remote");
  }

  @Test
  public void testPlanPrefetchPinsSmallEntriesAndPrimesOffsetTableOfLargeOnes()
      throws Exception {
    // Pin ceiling between the two dictionaries: the int dictionary (64 KB) is pinned whole, the var-length
    // string dictionary (~300 KB) only gets its offset table
    RemoteSegmentDirectory directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30),
        RemoteQueryConfigs.DEFAULT_MAX_FETCH_BYTES_PER_QUERY, /* maxPromoteBytes */ 1024, true, 100 * 1024);
    try {
      IndexKey intDictKey = new IndexKey(COLUMN, StandardIndexes.dictionary());
      IndexKey strDictKey = new IndexKey(STRING_COLUMN, StandardIndexes.dictionary());
      FetchContext fetchContext = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
          Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary()), STRING_COLUMN,
              List.<IndexType<?, ?, ?>>of(StandardIndexes.dictionary())));
      directory.prefetch(fetchContext);
      directory.acquire(fetchContext);
      assertNotNull(directory.awaitResident(intDictKey), "small dictionary must be pinned whole");
      assertNull(directory.peekResident(strDictKey), "large dictionary must not be pinned whole");
      PinotDataBuffer offsetTable = directory.offsetTable(strDictKey);
      assertNotNull(offsetTable, "large var-length dictionary must have its offset table primed");
      assertEquals(offsetTable.size(), (NUM_STRINGS + 1L) * Integer.BYTES);

      // Offset lookups are served from the table: resolving a value costs one ranged read at most (nothing is
      // promotable here, so a miss cannot hide behind a whole-entry promotion)
      SegmentDirectory.Reader reader = directory.createReader();
      RemoteSegmentBuffer strDict =
          (RemoteSegmentBuffer) reader.getIndexFor(STRING_COLUMN, StandardIndexes.dictionary());
      VarLengthValueReader valueReader = new VarLengthValueReader(strDict);
      long fetchesBefore = strDict.getRangeFetchCount();
      assertEquals(valueReader.getUnpaddedString(7, 64, new byte[64]), expectedString(7));
      assertTrue(strDict.getRangeFetchCount() - fetchesBefore <= 1,
          "with the offset table local a lookup is at most one ranged read");

      directory.release(fetchContext);
      assertFalse(directory.inQueryMode());
      assertNull(directory.peekResident(intDictKey));
    } finally {
      directory.close();
    }
  }

  @Test
  public void testHintedRangesAreFetchedAsynchronouslyAndReadsWaitForThem()
      throws Exception {
    // No query active and the entry is not promotable, so reads go through the ranged path
    RemoteSegmentDirectory directory = new RemoteSegmentDirectory(_remoteMetadata,
        new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30),
        RemoteQueryConfigs.DEFAULT_MAX_FETCH_BYTES_PER_QUERY, /* maxPromoteBytes */ 1024, false);
    try {
      SegmentDirectory.Reader reader = directory.createReader();
      RemoteSegmentBuffer fwd = (RemoteSegmentBuffer) reader.getIndexFor(COLUMN, StandardIndexes.forward());
      // Two clusters of values far apart: two merged ranges, one batch, in flight together
      long[] offsets = {0, 4, 8, 200_000L, 200_004L};
      int[] lengths = {4, 4, 4, 4, 4};
      fwd.prefetchRanges(offsets, lengths, offsets.length);
      assertEquals(fwd.getRangeFetchCount(), 2, "one request per merged range, issued at hint time");
      // Reads inside the hinted ranges wait for them instead of fetching again
      assertEquals(fwd.getInt(0), 3);
      assertEquals(fwd.getInt(8), 2 * 17 + 3);
      assertEquals(fwd.getInt(200_004L), (200_004 / 4) * 17 + 3);
      assertEquals(fwd.getRangeFetchCount(), 2, "hinted reads must not issue further requests");
      // A repeat hint for covered ranges is a no-op
      fwd.prefetchRanges(offsets, lengths, offsets.length);
      assertEquals(fwd.getRangeFetchCount(), 2);
    } finally {
      directory.close();
    }
  }

  @Test
  public void testReleaseWhileFetchInFlightClosesTheMappingOnceItLands()
      throws Exception {
    // A slow fetcher: the query releases before its pinned entry has landed
    String latencyKey =
        RemoteQueryConfigs.INSTANCE_PROPERTY_PREFIX + RemoteQueryConfigs.INSTANCE_FETCH_DEBUG_LATENCY_MS;
    System.setProperty(latencyKey, "400");
    RemoteIndexFetcher slowFetcher;
    try {
      slowFetcher = new RemoteIndexFetcher(4, RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, 30);
    } finally {
      System.clearProperty(latencyKey);
    }
    RemoteSegmentDirectory directory = new RemoteSegmentDirectory(_remoteMetadata, slowFetcher,
        RemoteQueryConfigs.DEFAULT_MAX_FETCH_BYTES_PER_QUERY, RemoteQueryConfigs.promoteMaxBytes(), true);
    try {
      long mappedBefore = PinotDataBuffer.getMmapBufferCount();
      IndexKey fwdKey = new IndexKey(COLUMN, StandardIndexes.forward());
      FetchContext fetchContext = new FetchContext(UUID.randomUUID(), SEGMENT_NAME,
          Map.of(COLUMN, List.<IndexType<?, ?, ?>>of(StandardIndexes.forward())));
      directory.prefetch(fetchContext);
      directory.acquire(fetchContext);
      assertTrue(directory.hasResident(fwdKey), "entry must be in flight");
      assertNull(directory.peekResident(fwdKey), "entry must not have landed yet");
      directory.release(fetchContext);
      assertFalse(directory.hasResident(fwdKey), "release must drop the in-flight entry");
      // The fetch still completes; its mapping must be closed by the completion, not left to leak
      long deadline = System.currentTimeMillis() + 5_000;
      while (PinotDataBuffer.getMmapBufferCount() != mappedBefore && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertEquals(PinotDataBuffer.getMmapBufferCount(), mappedBefore, "mapping of the abandoned fetch must be closed");
    } finally {
      directory.close();
    }
  }
}
