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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes ranged reads against the deep store for remote segments: sorts and coalesces requested ranges
 * (gaps smaller than the configured threshold are merged into a single GET), fetches on a bounded shared
 * pool, and hands each caller back exactly the bytes of its range.
 *
 * <p>Thread-safety: safe for concurrent use; the pool is shared per server process.
 */
public class RemoteIndexFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteIndexFetcher.class);

  private static volatile RemoteIndexFetcher _instance;

  private final ExecutorService _executor;
  /**
   * Pool for whole-entry orchestration tasks, which block on {@link #fetchRanges} — kept separate from the
   * bounded GET pool so orchestration can never starve the GET tasks it waits on (deadlock).
   */
  private final ExecutorService _coordinatorExecutor;
  private final long _coalesceGapBytes;
  private final int _fetchTimeoutSeconds;
  private final AtomicLong _fetchedBytes = new AtomicLong();
  private final AtomicLong _fetchCount = new AtomicLong();

  public RemoteIndexFetcher(int parallelism, long coalesceGapBytes, int fetchTimeoutSeconds) {
    _executor = Executors.newFixedThreadPool(parallelism, runnable -> {
      Thread thread = new Thread(runnable, "remote-index-fetcher");
      thread.setDaemon(true);
      return thread;
    });
    _coordinatorExecutor = Executors.newCachedThreadPool(runnable -> {
      Thread thread = new Thread(runnable, "remote-fetch-coordinator");
      thread.setDaemon(true);
      return thread;
    });
    _coalesceGapBytes = coalesceGapBytes;
    _fetchTimeoutSeconds = fetchTimeoutSeconds;
  }

  /** Executor for tasks that orchestrate fetches (and block on them); never runs the GETs themselves. */
  public ExecutorService getExecutor() {
    return _coordinatorExecutor;
  }

  /** Shared per-process instance with default sizing (overridable via {@link #init}). */
  public static RemoteIndexFetcher getInstance() {
    if (_instance == null) {
      synchronized (RemoteIndexFetcher.class) {
        if (_instance == null) {
          _instance = new RemoteIndexFetcher(RemoteQueryConfigs.DEFAULT_FETCH_PARALLELISM,
              RemoteQueryConfigs.DEFAULT_COALESCE_GAP_BYTES, RemoteQueryConfigs.DEFAULT_FETCH_TIMEOUT_SECONDS);
        }
      }
    }
    return _instance;
  }

  /** Installs a configured shared instance (server startup); later {@link #getInstance()} calls return it. */
  public static void init(int parallelism, long coalesceGapBytes, int fetchTimeoutSeconds) {
    synchronized (RemoteIndexFetcher.class) {
      _instance = new RemoteIndexFetcher(parallelism, coalesceGapBytes, fetchTimeoutSeconds);
    }
  }

  /** One requested byte range; {@code _target} receives exactly {@code _length} bytes on success. */
  public static class RangeRequest {
    private final long _offset;
    private final int _length;
    private final byte[] _target;
    private final int _targetOffset;

    public RangeRequest(long offset, int length, byte[] target, int targetOffset) {
      _offset = offset;
      _length = length;
      _target = target;
      _targetOffset = targetOffset;
    }
  }

  /**
   * Fetches all requested ranges of the given file, coalescing near-adjacent ranges into shared GETs.
   * Blocks until every range completed or the fetch timeout elapsed.
   *
   * @throws IOException when any range could not be read completely within the timeout
   */
  public void fetchRanges(URI fileUri, List<RangeRequest> requests)
      throws IOException {
    fetchRanges(fileUri, requests, _coalesceGapBytes);
  }

  /**
   * Same, with an explicit coalesce distance. A caller that has already merged its ranges passes 0, so the
   * groups here are exactly the GETs it planned and this call only parallelizes them.
   */
  public void fetchRanges(URI fileUri, List<RangeRequest> requests, long coalesceGapBytes)
      throws IOException {
    if (requests.isEmpty()) {
      return;
    }
    List<RangeRequest> sorted = new ArrayList<>(requests);
    sorted.sort(Comparator.comparingLong(request -> request._offset));

    // Coalesce into GET groups
    List<List<RangeRequest>> groups = new ArrayList<>();
    List<RangeRequest> current = new ArrayList<>();
    long currentEnd = -1;
    for (RangeRequest request : sorted) {
      if (current.isEmpty() || request._offset - currentEnd <= coalesceGapBytes) {
        current.add(request);
      } else {
        groups.add(current);
        current = new ArrayList<>();
        current.add(request);
      }
      currentEnd = Math.max(currentEnd, request._offset + request._length);
    }
    groups.add(current);

    PinotFS pinotFS = PinotFSFactory.create(fileUri.getScheme());
    List<CompletableFuture<Void>> futures = new ArrayList<>(groups.size());
    for (List<RangeRequest> group : groups) {
      futures.add(CompletableFuture.runAsync(() -> fetchGroup(pinotFS, fileUri, group), _executor));
    }
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(_fetchTimeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      futures.forEach(future -> future.cancel(true));
      throw new IOException("Remote range fetch failed for " + fileUri, e);
    }
  }

  private void fetchGroup(PinotFS pinotFS, URI fileUri, List<RangeRequest> group) {
    long groupStart = group.get(0)._offset;
    long groupEnd = 0;
    for (RangeRequest request : group) {
      groupEnd = Math.max(groupEnd, request._offset + request._length);
    }
    int groupLength = Math.toIntExact(groupEnd - groupStart);
    try {
      byte[] groupBuffer = new byte[groupLength];
      int read = pinotFS.readRange(fileUri, groupStart, groupBuffer, 0, groupLength);
      if (read < groupLength) {
        throw new IOException(
            "Short read: expected " + groupLength + " bytes at offset " + groupStart + " of " + fileUri + ", got "
                + read);
      }
      for (RangeRequest request : group) {
        System.arraycopy(groupBuffer, Math.toIntExact(request._offset - groupStart), request._target,
            request._targetOffset, request._length);
      }
      _fetchCount.incrementAndGet();
      _fetchedBytes.addAndGet(groupLength);
    } catch (IOException e) {
      LOGGER.warn("Failed to fetch range [{}, {}) of {}", groupStart, groupEnd, fileUri, e);
      throw new RuntimeException(e);
    }
  }

  public long getFetchedBytes() {
    return _fetchedBytes.get();
  }

  public long getFetchCount() {
    return _fetchCount.get();
  }
}
