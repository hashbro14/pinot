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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disk-backed cache of remote index entries.
 *
 * <p>An entry fetched from the deep store is streamed to a local file in fixed-size chunks and then
 * memory-mapped, so the JVM heap never holds the entry: the OS page cache owns the pages and reclaims them
 * under memory pressure. This is the same access path local Pinot segments use, so reads run at native speed,
 * and a second query touching the same entry is served from local disk with no deep-store traffic.
 *
 * <p>The cache is bounded by a byte budget ({@code pinot.server.instance.remote.cache.max.bytes}, default
 * 10&nbsp;GB): when a fetch would exceed it, least-recently-used entry files are deleted first. Deleting a
 * file that is still mapped is safe on POSIX — in-flight queries keep reading their mapping and the space is
 * reclaimed when it is unmapped.
 *
 * <p>Cache keys embed the segment CRC, so a refreshed segment can never be served from a stale file.
 *
 * <p>Thread-safety: safe for concurrent use; per-key materialization is serialized.
 */
public class RemoteEntryDiskCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteEntryDiskCache.class);

  public static final String CACHE_DIR_PROPERTY = "pinot.server.instance.remote.cache.dir";
  public static final String CACHE_MAX_BYTES_PROPERTY = "pinot.server.instance.remote.cache.max.bytes";
  public static final long DEFAULT_CACHE_MAX_BYTES = 10L << 30; // 10 GB
  /** Streaming chunk: bounds the transient heap used while writing an entry to disk. */
  static final int FETCH_CHUNK_BYTES = 8 << 20;

  private static volatile RemoteEntryDiskCache _instance;

  private final File _cacheDir;
  private final long _maxBytes;
  private final ConcurrentHashMap<String, CachedEntry> _entries = new ConcurrentHashMap<>();
  private final AtomicLong _cachedBytes = new AtomicLong();
  private final AtomicLong _hits = new AtomicLong();
  private final AtomicLong _misses = new AtomicLong();
  private final AtomicLong _evictions = new AtomicLong();
  private final AtomicLong _spilledBytes = new AtomicLong();
  private final AtomicLong _spillCounter = new AtomicLong();

  private static class CachedEntry {
    final File _file;
    final long _size;
    volatile long _lastAccessMs;

    CachedEntry(File file, long size) {
      _file = file;
      _size = size;
      _lastAccessMs = System.currentTimeMillis();
    }
  }

  /** Fetches {@code [offset, offset + length)} of an entry into {@code target}. */
  public interface RangeReader {
    void read(long offset, byte[] target, int length);
  }

  RemoteEntryDiskCache(File cacheDir, long maxBytes) {
    _cacheDir = cacheDir;
    _maxBytes = maxBytes;
    try {
      // Start from a clean slate: files from a previous run may belong to segments this server no longer holds
      FileUtils.deleteQuietly(_cacheDir);
      FileUtils.forceMkdir(_cacheDir);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot create remote entry cache dir: " + _cacheDir, e);
    }
    LOGGER.info("Remote entry disk cache at {} with budget {} bytes", _cacheDir, _maxBytes);
  }

  public static RemoteEntryDiskCache getInstance() {
    if (_instance == null) {
      synchronized (RemoteEntryDiskCache.class) {
        if (_instance == null) {
          String dir = System.getProperty(CACHE_DIR_PROPERTY,
              System.getProperty("java.io.tmpdir") + File.separator + "pinot-remote-cache");
          long maxBytes = DEFAULT_CACHE_MAX_BYTES;
          String configured = System.getProperty(CACHE_MAX_BYTES_PROPERTY);
          if (configured != null) {
            try {
              maxBytes = Long.parseLong(configured.trim());
            } catch (NumberFormatException ignored) {
              // keep default
            }
          }
          _instance = new RemoteEntryDiskCache(new File(dir), maxBytes);
        }
      }
    }
    return _instance;
  }

  /**
   * Returns a memory-mapped buffer over the entry, fetching and caching it on disk first when absent.
   * The returned buffer is big-endian, matching the {@code columns.psf} layout.
   */
  public PinotDataBuffer getOrFetch(String segmentKey, String entryName, long size, RangeReader reader) {
    String key = segmentKey + "::" + entryName;
    CachedEntry entry = _entries.compute(key, (k, current) -> {
      if (current != null && current._file.isFile() && current._file.length() == size) {
        current._lastAccessMs = System.currentTimeMillis();
        _hits.incrementAndGet();
        return current;
      }
      _misses.incrementAndGet();
      return fetchToDisk(k, size, reader);
    });
    evictIfNeeded(key);
    try {
      return PinotDataBuffer.mapFile(entry._file, /* readOnly */ true, 0, size, ByteOrder.BIG_ENDIAN,
          "remote-entry:" + key);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to map cached remote entry: " + key, e);
    }
  }

  /**
   * Streams an entry to a local file, memory-maps it and immediately unlinks the file. The mapping stays
   * valid for as long as the caller holds it (POSIX keeps the inode alive), so the JVM heap never holds the
   * entry and the disk space is reclaimed automatically when the mapping is closed at the end of the query —
   * including if the process dies, since an unlinked file has no name to leak.
   */
  public PinotDataBuffer fetchEphemeral(String segmentKey, String entryName, long size, RangeReader reader) {
    String key = segmentKey + "::" + entryName + "." + Thread.currentThread().getId() + "." + _spillCounter
        .incrementAndGet();
    CachedEntry entry = fetchToDisk(key, size, reader);
    try {
      PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(entry._file, /* readOnly */ true, 0, size, ByteOrder.BIG_ENDIAN,
              "remote-spill:" + key);
      // Unlink now: the mapping above keeps the data reachable, nothing else can find the file again
      FileUtils.deleteQuietly(entry._file);
      _cachedBytes.addAndGet(-size);
      _spilledBytes.addAndGet(size);
      return buffer;
    } catch (IOException e) {
      FileUtils.deleteQuietly(entry._file);
      _cachedBytes.addAndGet(-size);
      throw new IllegalStateException("Failed to map spilled remote entry: " + key, e);
    }
  }

  private CachedEntry fetchToDisk(String key, long size, RangeReader reader) {
    File file = new File(_cacheDir, sanitize(key));
    File tmp = new File(_cacheDir, sanitize(key) + ".tmp");
    try {
      // The cache dir can disappear under a running server (operator cleanup, tmp reaper): recreate it
      // rather than failing every query from then on.
      FileUtils.forceMkdir(_cacheDir);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot create remote entry cache dir: " + _cacheDir, e);
    }
    FileUtils.deleteQuietly(tmp);
    // Stream in chunks so a multi-GB entry never materializes on the heap
    byte[] chunk = new byte[(int) Math.min(FETCH_CHUNK_BYTES, size)];
    try (OutputStream out = Files.newOutputStream(tmp.toPath())) {
      long written = 0;
      while (written < size) {
        int length = (int) Math.min(chunk.length, size - written);
        reader.read(written, chunk, length);
        out.write(chunk, 0, length);
        written += length;
      }
    } catch (IOException e) {
      FileUtils.deleteQuietly(tmp);
      throw new IllegalStateException("Failed to stream remote entry to disk: " + key, e);
    }
    FileUtils.deleteQuietly(file);
    if (!tmp.renameTo(file)) {
      FileUtils.deleteQuietly(tmp);
      throw new IllegalStateException("Failed to publish cached remote entry: " + key);
    }
    _cachedBytes.addAndGet(size);
    return new CachedEntry(file, size);
  }

  /** Deletes least-recently-used entry files until the cache fits its budget again. */
  private void evictIfNeeded(String protectedKey) {
    if (_cachedBytes.get() <= _maxBytes) {
      return;
    }
    List<Map.Entry<String, CachedEntry>> candidates = _entries.entrySet().stream()
        .filter(e -> !e.getKey().equals(protectedKey))
        .sorted(Comparator.comparingLong(e -> e.getValue()._lastAccessMs))
        .collect(Collectors.toList());
    for (Map.Entry<String, CachedEntry> candidate : candidates) {
      if (_cachedBytes.get() <= _maxBytes) {
        break;
      }
      CachedEntry removed = _entries.remove(candidate.getKey());
      if (removed == null) {
        continue;
      }
      // Safe while mapped: POSIX keeps the inode alive for existing mappings, space is freed on unmap
      FileUtils.deleteQuietly(removed._file);
      _cachedBytes.addAndGet(-removed._size);
      _evictions.incrementAndGet();
      LOGGER.info("Evicted cached remote entry {} ({} bytes); cache now {} bytes", candidate.getKey(),
          removed._size, _cachedBytes.get());
    }
  }

  /** Drops all cached files of a segment (segment deleted or refreshed to a new CRC). */
  public void invalidateSegment(String segmentKey) {
    _entries.entrySet().removeIf(entry -> {
      if (!entry.getKey().startsWith(segmentKey + "::")) {
        return false;
      }
      FileUtils.deleteQuietly(entry.getValue()._file);
      _cachedBytes.addAndGet(-entry.getValue()._size);
      return true;
    });
  }

  private static String sanitize(String key) {
    return key.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  public long getCachedBytes() {
    return _cachedBytes.get();
  }

  public long getHits() {
    return _hits.get();
  }

  public long getMisses() {
    return _misses.get();
  }

  public long getEvictions() {
    return _evictions.get();
  }

  /** Total bytes spilled to disk for query-scoped use (files already unlinked). */
  public long getSpilledBytes() {
    return _spilledBytes.get();
  }
}
