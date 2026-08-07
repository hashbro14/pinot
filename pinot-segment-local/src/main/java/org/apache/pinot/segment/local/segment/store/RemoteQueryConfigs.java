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
  public static final int DEFAULT_FETCH_PARALLELISM = 8;
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
    String mode = System.getProperty("pinot.server.instance." + INSTANCE_STORAGE_MODE);
    if (mode != null) {
      try {
        return StorageMode.valueOf(mode.trim().toUpperCase(java.util.Locale.ROOT));
      } catch (IllegalArgumentException e) {
        return StorageMode.SPILL;
      }
    }
    // Backwards compatibility with the older boolean flag
    String cacheEnabled = System.getProperty("pinot.server.instance." + INSTANCE_CACHE_ENABLED);
    if (cacheEnabled != null) {
      return Boolean.parseBoolean(cacheEnabled.trim()) ? StorageMode.CACHE : StorageMode.HEAP;
    }
    return StorageMode.SPILL;
  }

  /** False disables the disk cache so every query fetches from the deep store (benchmark mode). */
  public static boolean diskCacheEnabled() {
    String override = System.getProperty("pinot.server.instance." + INSTANCE_CACHE_ENABLED);
    return override == null || Boolean.parseBoolean(override.trim());
  }

  /** Largest index entry fetched whole; overridable with the matching system property. */
  public static final String INSTANCE_EAGER_PREFETCH_ENABLED = "remote.eager.prefetch.enabled";

  /**
   * Whether acquiring a segment should pull every planned index entry whole, up to the promote ceiling.
   *
   * <p>Off by default. Readers now hand the buffer the batch of docIds or dictIds they are about to
   * resolve, so a selective query fetches just those ranges; pulling whole entries up front would read
   * hundreds of megabytes to answer a ten-row lookup. Queries that really do touch a whole entry still
   * get it in one read, via the promote-on-miss path. Turn this on to restore the eager behaviour.
   */
  public static boolean eagerPrefetchEnabled() {
    return Boolean.parseBoolean(
        System.getProperty("pinot.server.instance." + INSTANCE_EAGER_PREFETCH_ENABLED, "false"));
  }

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

  public static long promoteMaxBytes() {
    return longProperty(INSTANCE_PROMOTE_MAX_BYTES, DEFAULT_PROMOTE_MAX_BYTES);
  }

  private static long longProperty(String key, long defaultValue) {
    String override = System.getProperty("pinot.server.instance." + key);
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
