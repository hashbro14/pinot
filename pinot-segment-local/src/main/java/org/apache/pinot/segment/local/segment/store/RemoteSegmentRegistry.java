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
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-server registry of remote segments. Registration happens once per segment (at load): only the metadata
 * trio — {@code metadata.properties}, {@code index_map}, {@code creation.meta} — is copied from the deep store
 * into a small local scratch directory, parsed, and kept in memory. No segment data bytes are ever downloaded.
 *
 * <p>Entries are keyed by segment name; re-registration with a different CRC replaces the entry (segment
 * refresh). Thread-safety: concurrent registration of the same segment is serialized per key.
 */
public class RemoteSegmentRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSegmentRegistry.class);
  private static final String V3_SUBDIR = "v3";
  private static final String METADATA_FILE = "metadata.properties";
  private static final String CREATION_META_FILE = "creation.meta";

  private static final RemoteSegmentRegistry INSTANCE = new RemoteSegmentRegistry();

  private final ConcurrentHashMap<String, RemoteSegmentMetadata> _segments = new ConcurrentHashMap<>();

  public static RemoteSegmentRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the registered entry for the segment, fetching and parsing its remote metadata when absent or when
   * the CRC changed (refresh). {@code localScratchDir} is the segment's local directory: only the KB-sized
   * metadata files are placed under it.
   */
  public RemoteSegmentMetadata getOrRegister(String tableNameWithType, String segmentName, String crc,
      URI segmentBaseUri, File localScratchDir) {
    String key = registryKey(tableNameWithType, segmentName);
    RemoteSegmentMetadata existing = _segments.get(key);
    if (existing != null && existing.getCrc().equals(crc)) {
      return existing;
    }
    return _segments.compute(key, (k, current) -> {
      if (current != null && current.getCrc().equals(crc)) {
        return current;
      }
      try {
        return register(segmentName, crc, segmentBaseUri, localScratchDir);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to register remote segment: " + segmentName + " from " + segmentBaseUri, e);
      }
    });
  }

  public RemoteSegmentMetadata get(String tableNameWithType, String segmentName) {
    return _segments.get(registryKey(tableNameWithType, segmentName));
  }

  public void remove(String tableNameWithType, String segmentName) {
    RemoteSegmentMetadata removed = _segments.remove(registryKey(tableNameWithType, segmentName));
    if (removed != null) {
      FileUtils.deleteQuietly(removed.getLocalMetadataDir());
    }
  }

  private static String registryKey(String tableNameWithType, String segmentName) {
    return tableNameWithType + "::" + segmentName;
  }

  public int size() {
    return _segments.size();
  }

  private RemoteSegmentMetadata register(String segmentName, String crc, URI segmentBaseUri, File localScratchDir)
      throws Exception {
    String base = segmentBaseUri.toString();
    if (!base.endsWith("/")) {
      base += "/";
    }
    PinotFS pinotFS = PinotFSFactory.create(segmentBaseUri.getScheme());

    File v3Dir = new File(localScratchDir, V3_SUBDIR);
    FileUtils.deleteQuietly(localScratchDir);
    FileUtils.forceMkdir(v3Dir);

    // The metadata trio lives in the v3 subdirectory of the remote (untarred) segment layout
    pinotFS.copyToLocalFile(URI.create(base + V3_SUBDIR + "/" + METADATA_FILE), new File(v3Dir, METADATA_FILE));
    pinotFS.copyToLocalFile(URI.create(base + V3_SUBDIR + "/" + V1Constants.INDEX_MAP_FILE_NAME),
        new File(v3Dir, V1Constants.INDEX_MAP_FILE_NAME));
    pinotFS.copyToLocalFile(URI.create(base + V3_SUBDIR + "/" + CREATION_META_FILE),
        new File(v3Dir, CREATION_META_FILE));

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(localScratchDir);
    Map<IndexKey, RemoteSegmentMetadata.IndexRange> indexRanges =
        RemoteSegmentMetadata.parseIndexMap(new File(v3Dir, V1Constants.INDEX_MAP_FILE_NAME), localScratchDir);

    RemoteSegmentMetadata metadata = new RemoteSegmentMetadata(segmentName, crc, URI.create(base),
        RemoteSegmentMetadata.columnsPsfUri(segmentBaseUri), indexRanges, segmentMetadata, localScratchDir);
    LOGGER.info("Registered remote segment: {} (crc: {}, indexEntries: {}, totalIndexBytes: {}) from {}",
        segmentName, crc, indexRanges.size(), metadata.getTotalIndexBytes(), segmentBaseUri);
    return metadata;
  }
}
