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

  public static final long DEFAULT_MAX_FETCH_BYTES_PER_QUERY = 1L << 30; // 1 GB
  public static final int DEFAULT_FETCH_PARALLELISM = 8;
  public static final int DEFAULT_FETCH_TIMEOUT_SECONDS = 30;
  public static final long DEFAULT_COALESCE_GAP_BYTES = 256 * 1024;

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
    String baseUri = customConfig.getCustomConfigs().get(TABLE_REMOTE_QUERY_BASE_URI);
    if (baseUri == null || baseUri.isEmpty()) {
      throw new IllegalStateException(
          "Table " + tableConfig.getTableName() + " enables " + TABLE_REMOTE_QUERY_ENABLED + " but is missing "
              + TABLE_REMOTE_QUERY_BASE_URI);
    }
    return new RemoteQueryConfigs(URI.create(baseUri));
  }
}
