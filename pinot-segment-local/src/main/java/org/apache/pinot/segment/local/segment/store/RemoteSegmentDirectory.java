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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only {@link SegmentDirectory} whose data lives entirely in the deep store: index buffers are fetched
 * from {@code columns.psf} via ranged reads at query time and held only in memory, only for the duration of
 * the queries that need them. Nothing is persisted locally except the KB-sized metadata files handled by
 * {@link RemoteSegmentRegistry} — plus the segment's star-tree files when it has any, which live outside
 * {@code columns.psf} and are served memory-mapped from the same local scratch directory.
 *
 * <p>Lifecycle per query: {@link #prefetch} starts async fetches of the query's index entries and pins them,
 * {@link #acquire} marks the query active (reads wait for exactly the entry they touch, see
 * {@link #awaitResident}), {@link #release} unpins them — an entry whose pin count reaches zero is freed.
 * Reads that miss (planner under-fetch) promote the whole entry synchronously
 * while any query is active ("query mode"); reads with no active query (reader construction at segment load)
 * are served as small exact ranged reads through the buffer's bounded header cache, so loading a remote
 * segment never fetches index data wholesale.
 *
 * <p>Thread-safety: safe for concurrent queries; entry state is managed with per-key atomics.
 */
public class RemoteSegmentDirectory extends SegmentDirectory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSegmentDirectory.class);

  private final RemoteSegmentMetadata _metadata;
  private final RemoteIndexFetcher _fetcher;
  /**
   * Serves star-tree buffers, shared with (and owned by) the registry entry — see
   * {@link RemoteSegmentMetadata#getStarTreeIndexReader()}. Null when the segment has no star-trees or they
   * could not be loaded — queries then fall back to raw scans.
   */
  @Nullable
  private final StarTreeIndexReader _starTreeIndexReader;
  /** Per-query ceiling on bytes made resident by prefetch; beyond it the query reads lazily instead. */
  private final long _maxFetchBytesPerQuery;
  /** Largest single index entry that a query-mode miss may promote wholesale. */
  private final long _maxPromoteBytes;
  /** Largest index entry that plan-time prefetch pins whole; bigger entries are read in ranges on demand. */
  private final long _pinMaxBytes;
  /** Whether plan-time prefetch (pinning the query's index entries before execution) is enabled. */
  private final boolean _planPrefetch;

  /** Entries currently resident in memory, keyed by index entry. */
  private final ConcurrentHashMap<IndexKey, ResidentEntry> _resident = new ConcurrentHashMap<>();
  /**
   * Offset tables of var-length dictionaries too big to pin whole, keyed by the dictionary entry. Materialized
   * like resident entries (disk cache, mmap) so they cost no heap; the buffer is null for fixed-width
   * dictionaries, whose value positions are arithmetic.
   */
  private final ConcurrentHashMap<IndexKey, ResidentEntry> _offsetTables = new ConcurrentHashMap<>();
  /** Keys pinned by each in-flight fetch context. */
  private final ConcurrentHashMap<UUID, Set<IndexKey>> _pinsByContext = new ConcurrentHashMap<>();
  /** Offset tables pinned by each in-flight fetch context. */
  private final ConcurrentHashMap<UUID, Set<IndexKey>> _offsetPinsByContext = new ConcurrentHashMap<>();
  /**
   * Parsed header of each var-length dictionary entry: {offset table start, offset table bytes}; start is -1
   * for fixed-width dictionaries. Read once per entry and kept: it is 16 bytes of truth about the layout.
   */
  private final ConcurrentHashMap<IndexKey, int[]> _varLengthHeaders = new ConcurrentHashMap<>();
  /**
   * Contexts between acquire and their first release. Upstream may call release more than once for the same
   * context (and prefetch-only contexts are released without an acquire), so the active count is driven by
   * membership changes on this set, never by bare increments.
   */
  private final Set<UUID> _acquiredContexts = ConcurrentHashMap.newKeySet();
  private final AtomicInteger _activeContexts = new AtomicInteger();
  private final AtomicInteger _syncMisses = new AtomicInteger();
  private final AtomicInteger _degradedFetches = new AtomicInteger();

  private volatile String _tier;

  private static class ResidentEntry {
    final CompletableFuture<PinotDataBuffer> _data;
    final AtomicInteger _pins = new AtomicInteger();

    ResidentEntry(CompletableFuture<PinotDataBuffer> data) {
      _data = data;
    }
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher) {
    this(metadata, fetcher, RemoteQueryConfigs.DEFAULT_MAX_FETCH_BYTES_PER_QUERY);
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher,
      long maxFetchBytesPerQuery) {
    this(metadata, fetcher, maxFetchBytesPerQuery, RemoteQueryConfigs.promoteMaxBytes());
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher,
      long maxFetchBytesPerQuery, long maxPromoteBytes) {
    this(metadata, fetcher, maxFetchBytesPerQuery, maxPromoteBytes, RemoteQueryConfigs.planPrefetchEnabled());
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher,
      long maxFetchBytesPerQuery, long maxPromoteBytes, boolean planPrefetch) {
    this(metadata, fetcher, maxFetchBytesPerQuery, maxPromoteBytes, planPrefetch, RemoteQueryConfigs.pinMaxBytes());
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher,
      long maxFetchBytesPerQuery, long maxPromoteBytes, boolean planPrefetch, long pinMaxBytes) {
    _planPrefetch = planPrefetch;
    _metadata = metadata;
    _fetcher = fetcher;
    _maxFetchBytesPerQuery = maxFetchBytesPerQuery;
    _maxPromoteBytes = maxPromoteBytes;
    _pinMaxBytes = pinMaxBytes;
    _starTreeIndexReader = metadata.getStarTreeIndexReader();
  }

  /** Largest index entry a query-mode miss may promote wholesale; bigger entries are read in ranges. */
  long getMaxPromoteBytes() {
    return _maxPromoteBytes;
  }

  @Override
  public URI getIndexDir() {
    return _metadata.getSegmentBaseUri();
  }

  @Override
  public SegmentMetadataImpl getSegmentMetadata() {
    return _metadata.getSegmentMetadata();
  }

  @Override
  public void reloadMetadata()
      throws Exception {
    // Metadata is refreshed through RemoteSegmentRegistry on CRC change; nothing to reload in place.
  }

  @Override
  public Path getPath() {
    return _metadata.getLocalMetadataDir().toPath();
  }

  @Override
  public long getDiskSizeBytes() {
    // Nothing lives on disk; report the remote index size so table-size accounting stays meaningful.
    return _metadata.getTotalIndexBytes();
  }

  @Override
  public Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type) {
    Set<String> columns = new HashSet<>();
    for (IndexKey key : _metadata.getIndexRanges().keySet()) {
      if (key._type == type) {
        columns.add(key._name);
      }
    }
    return columns;
  }

  @Override
  public String getTier() {
    return _tier;
  }

  @Override
  public void setTier(@Nullable String tier) {
    _tier = tier;
  }

  /**
   * Plan-time prefetch: starts fetching, for this query, every index entry the planner says the query will
   * touch — before any segment operator runs, and for every segment of the query at once. Against object
   * storage that is the difference between one round trip and a chain of them: without it each segment's
   * operator discovers its dictionaries, filter indexes and forward indexes one dependent read at a time.
   *
   * <p>Policy per planned entry, in priority order (dictionaries and filter indexes first, forward indexes
   * last, so the per-query budget goes to what sits on the critical path):
   * <ul>
   *   <li>Small enough to pin ({@code <= pinMaxBytes}): materialized whole through the disk cache, in
   *   parallel with everything else. Small structures are exactly the ones that are read many times by
   *   dependent lookups (binary searches in dictionaries, sorted-index seeks), so they are worth having
   *   local in full.</li>
   *   <li>A var-length dictionary too big to pin: only its offset table is materialized. Resolving one value
   *   is otherwise three dependent reads; with the table local it is one.</li>
   *   <li>Anything else is left to the readers' batch hints and ranged reads.</li>
   * </ul>
   * Nothing here blocks: fetches are dispatched and joined by the first read that needs them, so a segment
   * starts its filter as soon as its filter indexes land rather than after its last forward index.
   */
  @Override
  public void prefetch(FetchContext fetchContext) {
    if (!_planPrefetch) {
      // Nothing is pulled up front: the operators hand each buffer the batch of docIds/dictIds they are
      // about to resolve, and that drives the fetching. An entry a query really does read end to end is
      // still fetched whole, on its first miss.
      return;
    }
    List<IndexKey> planned = plannedKeys(fetchContext);
    if (planned.isEmpty()) {
      return;
    }
    // Critical-path structures first; forward indexes are the bulk and are read once, so they go last
    planned.sort((a, b) -> Boolean.compare(a._type == StandardIndexes.forward(),
        b._type == StandardIndexes.forward()));
    UUID fetchId = fetchContext.getFetchId();
    Set<IndexKey> pinned = _pinsByContext.computeIfAbsent(fetchId, id -> ConcurrentHashMap.newKeySet());
    long plannedBytes = 0;
    for (IndexKey key : planned) {
      long size = indexRange(key).getDataSize();
      if (size > _pinMaxBytes) {
        // Too big to hold whole for a query that may touch a few rows of it. A var-length dictionary still
        // gets its offset table primed, which is what turns its lookups from three round trips into one.
        if (key._type == StandardIndexes.dictionary()) {
          Set<IndexKey> offsetPinned =
              _offsetPinsByContext.computeIfAbsent(fetchId, id -> ConcurrentHashMap.newKeySet());
          if (offsetPinned.add(key)) {
            pinOffsetTable(key);
          }
        }
        continue;
      }
      // Admission control: an entry that does not fit the per-query budget is served lazily by ranged reads
      // (smaller ones behind it may still fit). Memory stays bounded; the query is slower but correct, and never
      // takes the server down.
      if (plannedBytes + size > _maxFetchBytesPerQuery) {
        if (!pinned.contains(key)) {
          _degradedFetches.incrementAndGet();
          LOGGER.debug("Per-query prefetch budget of {} bytes reached for segment: {}; {} is read lazily",
              _maxFetchBytesPerQuery, _metadata.getSegmentName(), key);
        }
        continue;
      }
      plannedBytes += size;
      if (pinned.add(key)) {
        pin(key);
      }
    }
  }

  /** Index entries this fetch context asks for, restricted to entries the segment actually has. */
  private List<IndexKey> plannedKeys(FetchContext fetchContext) {
    List<IndexKey> planned = new ArrayList<>();
    for (Map.Entry<String, List<IndexType<?, ?, ?>>> entry : fetchContext.getColumnToIndexList().entrySet()) {
      for (IndexKey key : _metadata.getIndexRanges().keySet()) {
        if (!key._name.equals(entry.getKey())) {
          continue;
        }
        if (entry.getValue() != null && !entry.getValue().contains(key._type)) {
          continue;
        }
        planned.add(key);
      }
    }
    return planned;
  }

  @Override
  public void acquire(FetchContext fetchContext) {
    if (_acquiredContexts.add(fetchContext.getFetchId())) {
      _activeContexts.incrementAndGet();
    }
    // Ensure the context's entries are in flight (acquire may be called without a prior prefetch). Nothing is
    // joined here: the first read of each entry waits for exactly that entry, see awaitResident.
    prefetch(fetchContext);
  }

  @Override
  public void release(FetchContext fetchContext) {
    Set<IndexKey> pinned = _pinsByContext.remove(fetchContext.getFetchId());
    if (pinned != null) {
      for (IndexKey key : pinned) {
        unpin(_resident, key);
      }
    }
    Set<IndexKey> offsetPinned = _offsetPinsByContext.remove(fetchContext.getFetchId());
    if (offsetPinned != null) {
      for (IndexKey key : offsetPinned) {
        unpin(_offsetTables, key);
      }
    }
    // Idempotent: only the release matching a prior acquire drives the counter (upstream releases twice)
    if (_acquiredContexts.remove(fetchContext.getFetchId()) && _activeContexts.decrementAndGet() == 0) {
      freeStrays();
    }
  }

  /** True while at least one query is executing against this segment. */
  boolean inQueryMode() {
    return _activeContexts.get() > 0;
  }

  int getSyncMisses() {
    return _syncMisses.get();
  }

  int getDegradedFetches() {
    return _degradedFetches.get();
  }

  RemoteSegmentMetadata getRemoteMetadata() {
    return _metadata;
  }

  private void pin(IndexKey key) {
    ResidentEntry entry = _resident.compute(key, (k, current) -> {
      // Retry entries whose fetch failed instead of serving the poisoned future forever
      if (current != null && current._data.isCompletedExceptionally()) {
        current = null;
      }
      return current != null ? current
          : new ResidentEntry(CompletableFuture.supplyAsync(() -> fetchWholeEntry(k), _fetcher.getExecutor()));
    });
    entry._pins.incrementAndGet();
  }

  private void pinOffsetTable(IndexKey key) {
    ResidentEntry entry = _offsetTables.compute(key, (k, current) -> {
      if (current != null && current._data.isCompletedExceptionally()) {
        current = null;
      }
      return current != null ? current
          : new ResidentEntry(CompletableFuture.supplyAsync(() -> fetchOffsetTable(k), _fetcher.getExecutor()));
    });
    entry._pins.incrementAndGet();
  }

  private static void unpin(ConcurrentHashMap<IndexKey, ResidentEntry> entries, IndexKey key) {
    ResidentEntry entry = entries.get(key);
    if (entry != null && entry._pins.decrementAndGet() <= 0 && entries.remove(key, entry)) {
      closeQuietly(entry);
    }
  }

  /**
   * Releases an entry's mapping; the cached file stays for the next query to map again. An entry whose fetch is
   * still in flight (the query gave up before its bytes landed) is closed the moment it completes, so nothing is
   * left mapped with no owner.
   */
  private static void closeQuietly(ResidentEntry entry) {
    if (!entry._data.isDone()) {
      entry._data.whenComplete((buffer, error) -> closeBufferQuietly(buffer));
      return;
    }
    if (entry._data.isCompletedExceptionally()) {
      return;
    }
    closeBufferQuietly(entry._data.join());
  }

  private static void closeBufferQuietly(@Nullable PinotDataBuffer buffer) {
    if (buffer == null) {
      return;
    }
    try {
      buffer.close();
    } catch (Exception e) {
      LOGGER.warn("Failed to release mapped remote entry", e);
    }
  }

  /** Frees entries fetched by sync misses (pin count 0) once no query is active. */
  private void freeStrays() {
    freeStrays(_resident);
    freeStrays(_offsetTables);
  }

  private static void freeStrays(ConcurrentHashMap<IndexKey, ResidentEntry> entries) {
    entries.entrySet().removeIf(entry -> {
      if (entry.getValue()._pins.get() > 0) {
        return false;
      }
      closeQuietly(entry.getValue());
      return true;
    });
  }

  /**
   * Returns the resident buffer of the entry when already fetched, without fetching or waiting. Used by tests
   * and diagnostics; the buffers' read path uses {@link #awaitResident}.
   */
  @Nullable
  PinotDataBuffer peekResident(IndexKey key) {
    ResidentEntry entry = _resident.get(key);
    if (entry != null && entry._data.isDone() && !entry._data.isCompletedExceptionally()) {
      return entry._data.join();
    }
    return null;
  }

  /**
   * Returns the resident buffer of the entry, waiting for it when a fetch is in flight (plan-time prefetch
   * or another query's promotion); null when nothing is resident or the fetch failed, in which case the
   * caller falls back to ranged reads. Waiting beats reading around the fetch: the bytes are already on the
   * way, and a duplicate ranged read would only add requests.
   */
  @Nullable
  PinotDataBuffer awaitResident(IndexKey key) {
    ResidentEntry entry = _resident.get(key);
    if (entry == null) {
      return null;
    }
    try {
      return entry._data.join();
    } catch (RuntimeException e) {
      LOGGER.warn("Prefetch of {} failed for segment: {}; serving ranged reads instead", key,
          _metadata.getSegmentName(), e);
      return null;
    }
  }

  /**
   * Returns the materialized offset table of a var-length dictionary entry (mapped, big-endian, positioned at
   * {@link #offsetTableStart} within the entry's data), waiting for an in-flight fetch or fetching it now when
   * plan-time prefetch has not; null for fixed-width dictionaries and when the table could not be fetched. The
   * table is retained until every pinning query released it, or by the stray sweep when it was never pinned.
   */
  @Nullable
  PinotDataBuffer offsetTable(IndexKey key) {
    ResidentEntry entry = _offsetTables.get(key);
    if (entry == null || entry._data.isCompletedExceptionally()) {
      entry = _offsetTables.compute(key, (k, current) -> {
        if (current != null && !current._data.isCompletedExceptionally()) {
          return current;
        }
        return new ResidentEntry(
            CompletableFuture.supplyAsync(() -> fetchOffsetTable(k), _fetcher.getExecutor()));
      });
    }
    try {
      return entry._data.join();
    } catch (RuntimeException e) {
      LOGGER.warn("Offset table of {} could not be primed for segment: {}", key, _metadata.getSegmentName(), e);
      return null;
    }
  }

  /** Where the offset table starts within a var-length dictionary entry's data; valid after {@link #offsetTable}. */
  int offsetTableStart(IndexKey key) {
    return _varLengthHeaders.get(key)[0];
  }

  /**
   * Reads the 16-byte header of a var-length dictionary (magic, version, count, data start), remembers where its
   * offset table is and materializes the table. Returns null for fixed-width dictionaries.
   */
  @Nullable
  private PinotDataBuffer fetchOffsetTable(IndexKey key) {
    int[] header = _varLengthHeaders.get(key);
    if (header == null) {
      // The GET happens outside the map's compute so it never blocks other keys hashing to the same bin
      byte[] bytes = new byte[RemoteSegmentBuffer.VAR_LENGTH_HEADER_BYTES];
      readDataRange(key, 0, bytes, bytes.length);
      header = RemoteSegmentBuffer.parseVarLengthHeader(bytes, indexRange(key).getDataSize());
      _varLengthHeaders.putIfAbsent(key, header);
    }
    if (header[0] < 0) {
      return null;
    }
    int start = header[0];
    long tableBytes = header[1];
    RemoteEntryDiskCache.RangeReader reader =
        (offsetInTable, target, length) -> readDataRange(key, start + offsetInTable, target, length);
    return materialize(key._name + "." + key._type.getId() + ".offsets", tableBytes, reader);
  }

  /**
   * Starts materializing the whole entry without pinning it (freed by the stray sweep once no query is active),
   * for a hinted batch too scattered to fetch range by range. Reads that follow wait for it via
   * {@link #awaitResident}. No-op when the entry is already resident or in flight.
   */
  void promoteAsync(IndexKey key) {
    if (RemoteQueryConfigs.storageMode() == RemoteQueryConfigs.StorageMode.HEAP) {
      // A hinted promotion may run to hundreds of MBs; only disk-backed modes keep that off the JVM heap
      return;
    }
    _resident.compute(key, (k, current) -> {
      if (current != null && !current._data.isCompletedExceptionally()) {
        return current;
      }
      _syncMisses.incrementAndGet();
      return new ResidentEntry(CompletableFuture.supplyAsync(() -> fetchWholeEntry(k), _fetcher.getExecutor()));
    });
  }

  /** True when the entry is resident or being made resident (pinned or promoted); never waits. */
  boolean hasResident(IndexKey key) {
    ResidentEntry entry = _resident.get(key);
    return entry != null && !entry._data.isCompletedExceptionally();
  }

  /** The materialized offset table of a var-length dictionary when already fetched, else null; never waits. */
  @Nullable
  PinotDataBuffer peekOffsetTable(IndexKey key) {
    ResidentEntry entry = _offsetTables.get(key);
    if (entry != null && entry._data.isDone() && !entry._data.isCompletedExceptionally()) {
      return entry._data.join();
    }
    return null;
  }

  /** Runs a batch of ranged reads off the caller's thread; see {@link RemoteSegmentBuffer#prefetchRanges}. */
  CompletableFuture<Void> readDataRangesAsync(IndexKey key, List<long[]> ranges, List<byte[]> targets) {
    return CompletableFuture.runAsync(() -> readDataRanges(key, ranges, targets), _fetcher.getExecutor());
  }

  /**
   * Synchronously makes the entry resident (query-mode miss or a whole-buffer materialization) and returns
   * its buffer. The entry is freed once every pinning query released it, or by the stray sweep when it was
   * never pinned. A previously failed fetch is retried rather than served.
   */
  PinotDataBuffer promoteSync(IndexKey key) {
    _syncMisses.incrementAndGet();
    ResidentEntry entry = _resident.compute(key, (k, current) -> {
      if (current != null && !current._data.isCompletedExceptionally()) {
        return current;
      }
      return new ResidentEntry(CompletableFuture.completedFuture(fetchWholeEntry(k)));
    });
    return entry._data.join();
  }

  /** Fetches an exact sub-range of an entry's data (reader-construction reads at segment load). */
  void readDataRange(IndexKey key, long offsetInData, byte[] target, int length) {
    RemoteSegmentMetadata.IndexRange range = indexRange(key);
    List<RemoteIndexFetcher.RangeRequest> requests = new ArrayList<>(1);
    requests.add(new RemoteIndexFetcher.RangeRequest(range.getDataStartOffset() + offsetInData, length, target, 0));
    try {
      _fetcher.fetchRanges(_metadata.getColumnsPsfUri(), requests);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed ranged read of " + key + " [" + offsetInData + ", +" + length + ") for segment: "
              + _metadata.getSegmentName(), e);
    }
  }

  /**
   * Reads several ranges of one index entry with every range in flight at once.
   *
   * <p>Ranges are taken as already merged, so each becomes exactly one GET. Against object storage the
   * request count matters far less than whether the requests wait on each other: twenty concurrent ranged
   * reads cost about one round trip, twenty serial ones cost twenty.
   */
  void readDataRanges(IndexKey key, List<long[]> ranges, List<byte[]> targets) {
    RemoteSegmentMetadata.IndexRange range = indexRange(key);
    List<RemoteIndexFetcher.RangeRequest> requests = new ArrayList<>(ranges.size());
    for (int i = 0; i < ranges.size(); i++) {
      byte[] target = targets.get(i);
      requests.add(
          new RemoteIndexFetcher.RangeRequest(range.getDataStartOffset() + ranges.get(i)[0], target.length, target,
              0));
    }
    try {
      _fetcher.fetchRanges(_metadata.getColumnsPsfUri(), requests, 0);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed batched ranged read of " + key + " (" + ranges.size() + " ranges) for segment: "
              + _metadata.getSegmentName(), e);
    }
  }

  /**
   * Materializes a whole index entry through the disk cache: the bytes are streamed from the deep store into
   * a local file and memory-mapped, so the entry never occupies JVM heap and a later query touching the same
   * entry is served locally.
   */
  private PinotDataBuffer fetchWholeEntry(IndexKey key) {
    RemoteEntryDiskCache.RangeReader reader =
        (offsetInData, target, length) -> readDataRange(key, offsetInData, target, length);
    long size = indexRange(key).getDataSize();
    // Disk-backed modes stream the entry chunk by chunk, which the read-ahead overlaps; heap mode reads it in one
    // call and must see the plain reader
    if (size > RemoteEntryDiskCache.FETCH_CHUNK_BYTES
        && RemoteQueryConfigs.storageMode() != RemoteQueryConfigs.StorageMode.HEAP) {
      reader = new ReadAheadRangeReader(reader, size);
    }
    return materialize(key._name + "." + key._type.getId(), size, reader);
  }

  /**
   * Keeps the next chunks of a whole-entry stream in flight while the current one is written to disk, so a
   * multi-chunk entry costs about one round trip instead of one per chunk. The disk cache streams an entry
   * front to back in fixed chunks, which is the only access pattern this handles; anything else falls through
   * to the wrapped reader. Heap held at any time is bounded by the window.
   */
  private final class ReadAheadRangeReader implements RemoteEntryDiskCache.RangeReader {
    private static final int WINDOW = 4;
    private final RemoteEntryDiskCache.RangeReader _reader;
    private final long _size;
    private final Map<Long, CompletableFuture<byte[]>> _inFlight = new HashMap<>();

    ReadAheadRangeReader(RemoteEntryDiskCache.RangeReader reader, long size) {
      _reader = reader;
      _size = size;
    }

    @Override
    public void read(long offset, byte[] target, int length) {
      // Called from a single materializing thread, in order; no synchronization needed
      for (int i = 0; i < WINDOW; i++) {
        long next = offset + (long) i * RemoteEntryDiskCache.FETCH_CHUNK_BYTES;
        if (next >= _size || _inFlight.containsKey(next)) {
          continue;
        }
        int nextLength = (int) Math.min(RemoteEntryDiskCache.FETCH_CHUNK_BYTES, _size - next);
        _inFlight.put(next, CompletableFuture.supplyAsync(() -> {
          byte[] chunk = new byte[nextLength];
          _reader.read(next, chunk, nextLength);
          return chunk;
        }, _fetcher.getExecutor()));
      }
      CompletableFuture<byte[]> current = _inFlight.remove(offset);
      if (current == null) {
        _reader.read(offset, target, length);
        return;
      }
      byte[] chunk = current.join();
      if (chunk.length < length) {
        throw new IllegalStateException("Read-ahead chunk shorter than requested: " + chunk.length + " < " + length);
      }
      System.arraycopy(chunk, 0, target, 0, length);
    }
  }

  /**
   * Materializes {@code size} bytes served by {@code reader} under the configured storage mode: on the heap,
   * in the reusable disk cache, or spilled to an unlinked file — the latter two memory-mapped so the JVM heap
   * never holds the bytes.
   */
  private PinotDataBuffer materialize(String entryName, long size, RemoteEntryDiskCache.RangeReader reader) {
    String segmentKey = _metadata.getSegmentName() + ":" + _metadata.getCrc();
    switch (RemoteQueryConfigs.storageMode()) {
      case HEAP:
        // Nothing touches disk: the entry lives on the JVM heap until the query releases it.
        int dataSize = Math.toIntExact(size);
        byte[] data = new byte[dataSize];
        reader.read(0, data, dataSize);
        return PinotByteBuffer.wrap(ByteBuffer.wrap(data));
      case CACHE:
        return RemoteEntryDiskCache.getInstance().getOrFetch(segmentKey, entryName, size, reader);
      case SPILL:
      default:
        // Disk-backed but query-scoped: mapped from a file that is unlinked immediately, so heap stays flat
        // and the space is returned as soon as the query releases the mapping.
        return RemoteEntryDiskCache.getInstance().fetchEphemeral(segmentKey, entryName, size, reader);
    }
  }

  private RemoteSegmentMetadata.IndexRange indexRange(IndexKey key) {
    RemoteSegmentMetadata.IndexRange range = _metadata.getIndexRanges().get(key);
    if (range == null) {
      throw new IllegalStateException("No index entry: " + key + " in segment: " + _metadata.getSegmentName());
    }
    return range;
  }

  @Override
  public Reader createReader() {
    return new Reader() {
      @Override
      public PinotDataBuffer getIndexFor(String column, IndexType<?, ?, ?> type) {
        IndexKey key = new IndexKey(column, type);
        RemoteSegmentMetadata.IndexRange range = indexRange(key);
        return new RemoteSegmentBuffer(RemoteSegmentDirectory.this, key, 0, range.getDataSize(),
            ByteOrder.BIG_ENDIAN, type == StandardIndexes.dictionary());
      }

      @Override
      public boolean hasIndexFor(String column, IndexType<?, ?, ?> type) {
        return _metadata.getIndexRanges().containsKey(new IndexKey(column, type));
      }

      @Override
      public boolean hasStarTreeIndex() {
        return _starTreeIndexReader != null;
      }

      @Override
      public Reader getStarTreeIndexReader(int starTreeId) {
        return new Reader() {
          @Override
          public PinotDataBuffer getIndexFor(String column, IndexType<?, ?, ?> type)
              throws IOException {
            return _starTreeIndexReader.getBuffer(starTreeId, column, type);
          }

          @Override
          public boolean hasIndexFor(String column, IndexType<?, ?, ?> type) {
            return _starTreeIndexReader.hasIndexFor(starTreeId, column, type);
          }

          @Override
          public void close() {
          }

          @Override
          public String toString() {
            return _starTreeIndexReader + " for " + starTreeId;
          }
        };
      }

      @Override
      public void close() {
      }

      @Override
      public String toString() {
        return "RemoteSegmentDirectory.Reader:" + _metadata.getSegmentName();
      }
    };
  }

  @Override
  public Writer createWriter() {
    throw new UnsupportedOperationException("Remote segments are read-only: " + _metadata.getSegmentName());
  }

  @Override
  public String toString() {
    return "RemoteSegmentDirectory:" + _metadata.getSegmentName() + "@" + _metadata.getSegmentBaseUri();
  }

  @Override
  public void close()
      throws IOException {
    _resident.values().forEach(RemoteSegmentDirectory::closeQuietly);
    _resident.clear();
    _offsetTables.values().forEach(RemoteSegmentDirectory::closeQuietly);
    _offsetTables.clear();
    _pinsByContext.clear();
    _offsetPinsByContext.clear();
    // The star-tree reader is owned by the registry entry (shared across reloads of this segment) and is
    // closed on entry removal, not here.
  }
}
