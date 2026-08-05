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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.ColumnIndexUtils;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;

/**
 * Immutable description of one remote segment: where its {@code columns.psf} lives in the deep store and the
 * byte range of every (column, indexType) entry inside it, parsed from the segment's {@code index_map}.
 *
 * <p>Ranges are recorded as stored in the index map: they INCLUDE the leading 8-byte magic marker; the data
 * handed to index readers is {@code [startOffset + 8, startOffset + size)}, always big-endian (mirrors
 * {@code SingleFileIndexDirectory}).
 *
 * <p>Thread-safety: immutable after construction.
 */
public class RemoteSegmentMetadata {
  public static final long MAGIC_MARKER = 0xdeadbeefdeafbeadL;
  public static final int MAGIC_MARKER_SIZE_BYTES = 8;

  /** Byte range of one index entry within columns.psf, including the magic marker. */
  public static class IndexRange {
    private final long _startOffset;
    private final long _size;

    public IndexRange(long startOffset, long size) {
      _startOffset = startOffset;
      _size = size;
    }

    public long getStartOffset() {
      return _startOffset;
    }

    public long getSize() {
      return _size;
    }

    /** Start of the index data, past the magic marker. */
    public long getDataStartOffset() {
      return _startOffset + MAGIC_MARKER_SIZE_BYTES;
    }

    /** Size of the index data, without the magic marker. */
    public long getDataSize() {
      return _size - MAGIC_MARKER_SIZE_BYTES;
    }
  }

  private final String _segmentName;
  private final String _crc;
  private final URI _segmentBaseUri;
  private final URI _columnsPsfUri;
  private final Map<IndexKey, IndexRange> _indexRanges;
  private final SegmentMetadataImpl _segmentMetadata;
  private final File _localMetadataDir;
  private final long _totalIndexBytes;

  public RemoteSegmentMetadata(String segmentName, String crc, URI segmentBaseUri, URI columnsPsfUri,
      Map<IndexKey, IndexRange> indexRanges, SegmentMetadataImpl segmentMetadata, File localMetadataDir) {
    _segmentName = segmentName;
    _crc = crc;
    _segmentBaseUri = segmentBaseUri;
    _columnsPsfUri = columnsPsfUri;
    _indexRanges = Map.copyOf(indexRanges);
    _segmentMetadata = segmentMetadata;
    _localMetadataDir = localMetadataDir;
    _totalIndexBytes = indexRanges.values().stream().mapToLong(IndexRange::getSize).sum();
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getCrc() {
    return _crc;
  }

  public URI getSegmentBaseUri() {
    return _segmentBaseUri;
  }

  public URI getColumnsPsfUri() {
    return _columnsPsfUri;
  }

  public Map<IndexKey, IndexRange> getIndexRanges() {
    return _indexRanges;
  }

  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  public File getLocalMetadataDir() {
    return _localMetadataDir;
  }

  public long getTotalIndexBytes() {
    return _totalIndexBytes;
  }

  /**
   * Parses an {@code index_map} file into (column, indexType) byte ranges, using the same key parsing as
   * {@code SingleFileIndexDirectory#loadMap}. Unknown index types are skipped.
   */
  public static Map<IndexKey, IndexRange> parseIndexMap(File indexMapFile, File segmentDirectory)
      throws Exception {
    PropertiesConfiguration mapConfig = CommonsConfigurationUtils.fromFile(indexMapFile);
    Map<IndexKey, long[]> raw = new HashMap<>();
    for (String key : CommonsConfigurationUtils.getKeys(mapConfig)) {
      String[] parsedKeys = ColumnIndexUtils.parseIndexMapKeys(key, segmentDirectory.getPath());
      IndexKey indexKey;
      try {
        indexKey = IndexKey.fromIndexName(parsedKeys[0], parsedKeys[1]);
      } catch (IllegalArgumentException e) {
        continue;
      }
      long[] range = raw.computeIfAbsent(indexKey, k -> new long[]{-1, -1});
      if (parsedKeys[2].equals(ColumnIndexUtils.MAP_KEY_NAME_START_OFFSET)) {
        range[0] = mapConfig.getLong(key);
      } else if (parsedKeys[2].equals(ColumnIndexUtils.MAP_KEY_NAME_SIZE)) {
        range[1] = mapConfig.getLong(key);
      }
    }
    Map<IndexKey, IndexRange> ranges = new HashMap<>(raw.size());
    for (Map.Entry<IndexKey, long[]> entry : raw.entrySet()) {
      long startOffset = entry.getValue()[0];
      long size = entry.getValue()[1];
      if (startOffset < 0 || size < 0) {
        throw new IllegalStateException(
            "Invalid index map entry for " + entry.getKey() + " in " + indexMapFile);
      }
      ranges.put(entry.getKey(), new IndexRange(startOffset, size));
    }
    return ranges;
  }

  /** Location of the columns.psf single-file index within a segment's remote v3 directory. */
  public static URI columnsPsfUri(URI segmentBaseUri) {
    String base = segmentBaseUri.toString();
    if (!base.endsWith("/")) {
      base += "/";
    }
    return URI.create(base + V3_SUBDIRECTORY_NAME + "/" + V1Constants.INDEX_FILE_NAME);
  }

  private static final String V3_SUBDIRECTORY_NAME = "v3";
}
