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
 * {@link RemoteSegmentRegistry}.
 *
 * <p>Lifecycle per query: {@link #prefetch} starts async fetches of the query's index entries and pins them,
 * {@link #acquire} blocks until the pinned entries are resident, {@link #release} unpins them — an entry whose
 * pin count reaches zero is freed. Reads that miss (planner under-fetch) promote the whole entry synchronously
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

  /** Entries currently resident in memory, keyed by index entry. */
  private final ConcurrentHashMap<IndexKey, ResidentEntry> _resident = new ConcurrentHashMap<>();
  /** Keys pinned by each in-flight fetch context. */
  private final ConcurrentHashMap<UUID, Set<IndexKey>> _pinsByContext = new ConcurrentHashMap<>();
  /**
   * Contexts between acquire and their first release. Upstream may call release more than once for the same
   * context (and prefetch-only contexts are released without an acquire), so the active count is driven by
   * membership changes on this set, never by bare increments.
   */
  private final Set<UUID> _acquiredContexts = ConcurrentHashMap.newKeySet();
  private final AtomicInteger _activeContexts = new AtomicInteger();
  private final AtomicInteger _syncMisses = new AtomicInteger();

  private volatile String _tier;

  private static class ResidentEntry {
    final CompletableFuture<PinotDataBuffer> _data;
    final AtomicInteger _pins = new AtomicInteger();

    ResidentEntry(CompletableFuture<PinotDataBuffer> data) {
      _data = data;
    }
  }

  public RemoteSegmentDirectory(RemoteSegmentMetadata metadata, RemoteIndexFetcher fetcher) {
    _metadata = metadata;
    _fetcher = fetcher;
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

  @Override
  public void prefetch(FetchContext fetchContext) {
    Set<IndexKey> pinned =
        _pinsByContext.computeIfAbsent(fetchContext.getFetchId(), id -> ConcurrentHashMap.newKeySet());
    for (Map.Entry<String, List<IndexType<?, ?, ?>>> entry : fetchContext.getColumnToIndexList().entrySet()) {
      for (IndexKey key : _metadata.getIndexRanges().keySet()) {
        if (!key._name.equals(entry.getKey())) {
          continue;
        }
        if (entry.getValue() != null && !entry.getValue().contains(key._type)) {
          continue;
        }
        if (pinned.add(key)) {
          pin(key);
        }
      }
    }
  }

  @Override
  public void acquire(FetchContext fetchContext) {
    if (_acquiredContexts.add(fetchContext.getFetchId())) {
      _activeContexts.incrementAndGet();
    }
    // Ensure the context's entries are pinned (acquire may be called without a prior prefetch) and resident
    prefetch(fetchContext);
    Set<IndexKey> pinned = _pinsByContext.get(fetchContext.getFetchId());
    if (pinned == null) {
      return;
    }
    for (IndexKey key : pinned) {
      ResidentEntry entry = _resident.get(key);
      if (entry != null) {
        try {
          entry._data.join();
        } catch (RuntimeException e) {
          // A failed fetch surfaces at read time as a promoted retry; acquire must not fail the whole query
          LOGGER.warn("Prefetch of {} failed for segment: {}", key, _metadata.getSegmentName(), e);
        }
      }
    }
  }

  @Override
  public void release(FetchContext fetchContext) {
    Set<IndexKey> pinned = _pinsByContext.remove(fetchContext.getFetchId());
    if (pinned != null) {
      for (IndexKey key : pinned) {
        unpin(key);
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

  private void unpin(IndexKey key) {
    ResidentEntry entry = _resident.get(key);
    if (entry != null && entry._pins.decrementAndGet() <= 0) {
      _resident.remove(key, entry);
    }
  }

  /** Frees entries fetched by sync misses (pin count 0) once no query is active. */
  private void freeStrays() {
    _resident.entrySet().removeIf(entry -> entry.getValue()._pins.get() <= 0);
  }

  /**
   * Returns the resident buffer of the entry when already fetched, without fetching. Used as the buffers'
   * fast path.
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

  private PinotDataBuffer fetchWholeEntry(IndexKey key) {
    RemoteSegmentMetadata.IndexRange range = indexRange(key);
    int dataSize = Math.toIntExact(range.getDataSize());
    byte[] data = new byte[dataSize];
    List<RemoteIndexFetcher.RangeRequest> requests = new ArrayList<>(1);
    requests.add(new RemoteIndexFetcher.RangeRequest(range.getDataStartOffset(), dataSize, data, 0));
    try {
      _fetcher.fetchRanges(_metadata.getColumnsPsfUri(), requests);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to fetch index entry " + key + " for segment: " + _metadata.getSegmentName(), e);
    }
    return PinotByteBuffer.wrap(ByteBuffer.wrap(data));
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
            ByteOrder.BIG_ENDIAN);
      }

      @Override
      public boolean hasIndexFor(String column, IndexType<?, ?, ?> type) {
        return _metadata.getIndexRanges().containsKey(new IndexKey(column, type));
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
    _resident.clear();
    _pinsByContext.clear();
  }
}
