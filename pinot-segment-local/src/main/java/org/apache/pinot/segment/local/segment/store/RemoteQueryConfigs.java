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

import java.net.URI;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;

/**
 * Configuration for direct remote (deep store) querying of OFFLINE segments.
 *
 * <p>Table opt-in goes through {@code customConfigs} so no TableConfig schema change is needed:
 * <pre>
 * "customConfigs": {
 *   "customConfigs": {
 *     "remote.query.enabled": "true",
 *     "remote.query.base.uri": "s3://bucket/path/to/table/"
 *   }
 * }
 * </pre>
 * A segment named {@code seg} is expected at {@code <base.uri>/seg/v3/columns.psf} along with the
 * metadata files ({@code metadata.properties}, {@code index_map}, {@code creation.meta}) — the
 * untarred v3 layout, because compressed tarballs do not support ranged reads.
 *
 * <p>Thread-safety: immutable.
 */
public class RemoteQueryConfigs {
  /** Table-level custom config keys. */
  public static final String TABLE_REMOTE_QUERY_ENABLED = "remote.query.enabled";
  public static final String TABLE_REMOTE_QUERY_BASE_URI = "remote.query.base.uri";

  /** Instance-level (server.conf) keys, under pinot.server.instance. */
  public static final String INSTANCE_REMOTE_QUERY_ENABLED = "remote.query.enabled";
  public static final String INSTANCE_MAX_FETCH_BYTES_PER_QUERY = "remote.query.max.fetch.bytes";
  public static final String INSTANCE_FETCH_PARALLELISM = "remote.fetch.parallelism";
  public static final String INSTANCE_FETCH_TIMEOUT_SECONDS = "remote.fetch.timeout.seconds";
  public static final String INSTANCE_COALESCE_GAP_BYTES = "remote.coalesce.gap.bytes";
  /** Largest index entry fetched whole; bigger entries are read in ranges on demand. */
  public static final String INSTANCE_PROMOTE_MAX_BYTES = "remote.promote.max.bytes";
  /**
   * Set false to bypass the disk cache entirely: every query re-reads its bytes from the deep store, so
   * measurements reflect deep-store cost with no local reuse. Intended for benchmarking, not production.
   */
  public static final String INSTANCE_CACHE_ENABLED = "remote.cache.enabled";
  /**
   * How fetched index entries are held: {@code spill} (default) writes them to a local file, memory-maps it
   * and unlinks it immediately, so the JVM heap stays flat and the space is reclaimed the moment the query
   * releases the mapping; {@code cache} keeps the files for reuse across queries (LRU, bounded);
   * {@code heap} keeps entries on the JVM heap and never touches disk.
   */
  public static final String INSTANCE_STORAGE_MODE = "remote.storage.mode";

  public static final long DEFAULT_MAX_FETCH_BYTES_PER_QUERY = 1L << 30; // 1 GB
  /**
   * Threads available for ranged fetches.
   *
   * <p>Sized above the old default of 8 because a batch now submits all of its ranges at once and blocks
   * until they land. With a small shared pool, every operator thread queues its ranges behind every other
   * thread's while holding a thread hostage, which is slower than fetching one range at a time.
   */
  public static final int DEFAULT_FETCH_PARALLELISM = 32;
  public static final int DEFAULT_FETCH_TIMEOUT_SECONDS = 30;
  public static final long DEFAULT_COALESCE_GAP_BYTES = 256 * 1024;
  /**
   * Default ceiling for fetching an index entry whole. Above it, ranged reads win decisively: on a real
   * table a large string dictionary (390 MB) was being pulled in full to decode a 100-row result.
   */
  public static final long DEFAULT_PROMOTE_MAX_BYTES = 32L << 20;

  /**
   * Per-query budget for bytes made resident by prefetch. Overridable with the
   * {@code pinot.server.instance.remote.query.max.fetch.bytes} system property; a query planning more than
   * this is served with ranged reads instead of being materialized.
   */
  public static long maxFetchBytesPerQuery() {
    return longProperty(INSTANCE_MAX_FETCH_BYTES_PER_QUERY, DEFAULT_MAX_FETCH_BYTES_PER_QUERY);
  }

  /** How fetched entries are held for the duration of a query. */
  public enum StorageMode {
    /** Spill to a local file, mmap it, unlink at once: flat heap, space reclaimed when the query ends. */
    SPILL,
    /** Keep entry files for reuse across queries, bounded by a byte budget with LRU eviction. */
    CACHE,
    /** Keep entries on the JVM heap; nothing is written to disk. */
    HEAP
  }

  public static StorageMode storageMode() {
    String mode = System.getProperty(INSTANCE_PROPERTY_PREFIX + INSTANCE_STORAGE_MODE);
    if (mode != null) {
      try {
        return StorageMode.valueOf(mode.trim().toUpperCase(java.util.Locale.ROOT));
      } catch (IllegalArgumentException e) {
        return StorageMode.SPILL;
      }
    }
    // Backwards compatibility with the older boolean flag
    String cacheEnabled = System.getProperty(INSTANCE_PROPERTY_PREFIX + INSTANCE_CACHE_ENABLED);
    if (cacheEnabled != null) {
      return Boolean.parseBoolean(cacheEnabled.trim()) ? StorageMode.CACHE : StorageMode.HEAP;
    }
    return StorageMode.SPILL;
  }

  /** False disables the disk cache so every query fetches from the deep store (benchmark mode). */
  public static boolean diskCacheEnabled() {
    String override = System.getProperty(INSTANCE_PROPERTY_PREFIX + INSTANCE_CACHE_ENABLED);
    return override == null || Boolean.parseBoolean(override.trim());
  }

  public static final String INSTANCE_PLAN_PREFETCH_ENABLED = "remote.plan.prefetch.enabled";

  /**
   * Whether the index entries a query plans to touch are fetched before execution starts, for every segment
   * at once, instead of being discovered one dependent read at a time by each segment's operators.
   *
   * <p>On by default. What gets pulled whole is bounded by {@link #pinMaxBytes()} per entry and
   * {@link #maxFetchBytesPerQuery()} per query; a var-length dictionary above the pin ceiling gets only its
   * offset table. Turn this off to measure the lazy path.
   */
  public static boolean planPrefetchEnabled() {
    String value = System.getProperty(INSTANCE_PROPERTY_PREFIX + INSTANCE_PLAN_PREFETCH_ENABLED);
    if (value == null) {
      // The pre-rename key of the same switch; honoured so an explicit setting keeps working
      value = System.getProperty(INSTANCE_PROPERTY_PREFIX + LEGACY_EAGER_PREFETCH_ENABLED, "true");
    }
    return Boolean.parseBoolean(value);
  }

  /** Former name of {@link #INSTANCE_PLAN_PREFETCH_ENABLED}; read as a fallback only. */
  static final String LEGACY_EAGER_PREFETCH_ENABLED = "remote.eager.prefetch.enabled";

  public static final String INSTANCE_PIN_MAX_BYTES = "remote.pin.max.bytes";
  /**
   * Default pin ceiling. Sized so per-segment forward indexes and small dictionaries (single-digit MBs on a
   * day-sized segment) are pinned in one GET each, while the large string dictionaries a projection barely
   * touches (tens to hundreds of MBs) are left to their offset tables and ranged reads: pinning those whole
   * for a ten-row lookup measured as the dominant cost of {@code SELECT * LIMIT 10}.
   */
  public static final long DEFAULT_PIN_MAX_BYTES = 8L << 20;

  /** Largest index entry plan-time prefetch pins whole; never above the promote ceiling. */
  public static long pinMaxBytes() {
    return Math.min(longProperty(INSTANCE_PIN_MAX_BYTES, DEFAULT_PIN_MAX_BYTES), promoteMaxBytes());
  }

  public static final String INSTANCE_HINT_PROMOTE_MAX_BYTES = "remote.hint.promote.max.bytes";
  public static final long DEFAULT_HINT_PROMOTE_MAX_BYTES = 512L << 20;

  /**
   * Largest index entry a batch hint may materialize whole when its ranges are too scattered to coalesce
   * within budget. Above the promote ceiling on purpose: a hinted batch is a query that will keep coming back
   * for thousands of scattered values (a GROUP BY on a high-cardinality string column), and one streamed
   * transfer beats thousands of round trips even at hundreds of megabytes. Spill/cache modes keep it off heap.
   */
  public static long hintedPromoteMaxBytes() {
    return longProperty(INSTANCE_HINT_PROMOTE_MAX_BYTES, DEFAULT_HINT_PROMOTE_MAX_BYTES);
  }

  public static final String INSTANCE_PIPELINED_HINTS_ENABLED = "remote.pipelined.hints.enabled";

  /**
   * Whether a projection hints every column of a block — forward indexes first, then the dictionaries with
   * the ids those decode to — before reading any of them, so one block costs one or two round trips instead
   * of one or two per column. On by default; off restores per-column synchronous hinting, for measuring.
   */
  public static boolean pipelinedHintsEnabled() {
    return Boolean.parseBoolean(
        System.getProperty(INSTANCE_PROPERTY_PREFIX + INSTANCE_PIPELINED_HINTS_ENABLED, "true"));
  }

  public static final String INSTANCE_FETCH_DEBUG_LATENCY_MS = "remote.fetch.debug.latency.ms";

  /**
   * Artificial first-byte latency added to every ranged GET, for measuring round-trip counts against a local
   * deep store. Zero (the default) in production. Injected per request, not per link, so concurrent requests
   * overlap the way they do against real object storage — the property a network-level delay cannot model.
   */
  public static long fetchDebugLatencyMs() {
    return longProperty(INSTANCE_FETCH_DEBUG_LATENCY_MS, 0);
  }

  public static int fetchParallelism() {
    return (int) longProperty(INSTANCE_FETCH_PARALLELISM, DEFAULT_FETCH_PARALLELISM);
  }

  public static int fetchTimeoutSeconds() {
    return (int) longProperty(INSTANCE_FETCH_TIMEOUT_SECONDS, DEFAULT_FETCH_TIMEOUT_SECONDS);
  }

  public static long coalesceGapBytes() {
    return longProperty(INSTANCE_COALESCE_GAP_BYTES, DEFAULT_COALESCE_GAP_BYTES);
  }

  /** Prefix under which every instance-level key above is looked up as a system property. */
  public static final String INSTANCE_PROPERTY_PREFIX = "pinot.server.instance.";

  public static final String INSTANCE_PREFETCH_COALESCE_GAP_BYTES = "remote.prefetch.coalesce.gap.bytes";
  public static final long DEFAULT_PREFETCH_COALESCE_GAP_BYTES = 4L << 10; // 4 KB

  /**
   * How far apart two ranges of a batch can be and still be fetched as one.
   *
   * <p>This is the bytes-versus-round-trips dial. A batch of scattered rows leaves small holes between
   * the ranges it needs; merging across them reads bytes nobody asked for, but splitting on them costs
   * a round trip each. Against object storage, where a request costs tens of milliseconds and bandwidth
   * is cheap, merging generously wins — lower it when the deep store is nearer or rows are sparser.
   */
  public static long prefetchCoalesceGapBytes() {
    return longProperty(INSTANCE_PREFETCH_COALESCE_GAP_BYTES, DEFAULT_PREFETCH_COALESCE_GAP_BYTES);
  }

  public static final String INSTANCE_PREFETCH_MAX_BYTES = "remote.prefetch.max.bytes";
  public static final long DEFAULT_PREFETCH_MAX_BYTES = 32L << 20; // 32 MB

  /** Ceiling on what one batch may pull; past it the entry is read whole instead. */
  public static long prefetchMaxBytes() {
    return longProperty(INSTANCE_PREFETCH_MAX_BYTES, DEFAULT_PREFETCH_MAX_BYTES);
  }

  public static final String INSTANCE_PREFETCH_MAX_RANGES = "remote.prefetch.max.ranges";
  public static final int DEFAULT_PREFETCH_MAX_RANGES = 32;

  /**
   * Ceiling on the requests one batch may issue before prefetching gives up and the entry is read whole.
   *
   * <p>The two failure modes sit on either side of this. Too low and a scattered batch is declined, so
   * the query pays for whole entries it barely touches. Too high and a batch that cannot coalesce issues
   * a round trip per value --- and a query issues one batch per block, per column, per segment, so that
   * multiplies fast.
   */
  public static int prefetchMaxRanges() {
    return (int) longProperty(INSTANCE_PREFETCH_MAX_RANGES, DEFAULT_PREFETCH_MAX_RANGES);
  }

  public static long promoteMaxBytes() {
    return longProperty(INSTANCE_PROMOTE_MAX_BYTES, DEFAULT_PROMOTE_MAX_BYTES);
  }

  private static long longProperty(String key, long defaultValue) {
    String override = System.getProperty(INSTANCE_PROPERTY_PREFIX + key);
    if (override != null) {
      try {
        return Long.parseLong(override.trim());
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private final URI _baseUri;

  private RemoteQueryConfigs(URI baseUri) {
    _baseUri = baseUri;
  }

  public URI getBaseUri() {
    return _baseUri;
  }

  /** Resolves the deep-store directory of the given segment under this table's base URI. */
  public URI segmentBaseUri(String segmentName) {
    String base = _baseUri.toString();
    if (!base.endsWith("/")) {
      base += "/";
    }
    return URI.create(base + segmentName + "/");
  }

  /**
   * Extracts the remote-query config from the table config, or null when the table is not remote-query enabled.
   * Only OFFLINE tables may enable remote querying.
   */
  @Nullable
  public static RemoteQueryConfigs fromTableConfig(@Nullable TableConfig tableConfig) {
    if (tableConfig == null || tableConfig.getTableType() != TableType.OFFLINE) {
      return null;
    }
    TableCustomConfig customConfig = tableConfig.getCustomConfig();
    if (customConfig == null || customConfig.getCustomConfigs() == null) {
      return null;
    }
    if (!Boolean.parseBoolean(customConfig.getCustomConfigs().get(TABLE_REMOTE_QUERY_ENABLED))) {
      return null;
    }
    // Dimension tables are replicated to every server and read locally for lookup/broadcast joins; serving
    // them from the deep store would make every join stream them out of object storage.
    if (tableConfig.isDimTable()) {
      throw new IllegalStateException(
          "Table " + tableConfig.getTableName() + " is a dimension table and cannot enable "
              + TABLE_REMOTE_QUERY_ENABLED + ": dimension tables must be stored locally on every server");
    }
    String baseUri = customConfig.getCustomConfigs().get(TABLE_REMOTE_QUERY_BASE_URI);
    if (baseUri == null || baseUri.isEmpty()) {
      throw new IllegalStateException(
          "Table " + tableConfig.getTableName() + " enables " + TABLE_REMOTE_QUERY_ENABLED + " but is missing "
              + TABLE_REMOTE_QUERY_BASE_URI);
    }
    return new RemoteQueryConfigs(URI.create(baseUri));
  }
}
