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
package org.apache.pinot.segment.local.loader;

import java.io.File;
import java.net.URI;
import org.apache.pinot.segment.local.segment.store.RemoteIndexFetcher;
import org.apache.pinot.segment.local.segment.store.RemoteQueryConfigs;
import org.apache.pinot.segment.local.segment.store.RemoteSegmentDirectory;
import org.apache.pinot.segment.local.segment.store.RemoteSegmentMetadata;
import org.apache.pinot.segment.local.segment.store.RemoteSegmentRegistry;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentLoader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SegmentDirectoryLoader} for remote-query tables: opens segments whose data stays in the deep store.
 * Only the metadata trio is fetched (once, into the segment's local directory) — index data is read from the
 * deep store at query time via ranged reads.
 *
 * <p>Tables that do not enable remote querying fall through to {@link DefaultSegmentDirectoryLoader}, so an
 * instance configured with this loader behaves exactly as today for every non-remote table.
 */
@SegmentLoader(name = "remote")
public class RemoteSegmentDirectoryLoader implements SegmentDirectoryLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSegmentDirectoryLoader.class);

  private final DefaultSegmentDirectoryLoader _defaultLoader = new DefaultSegmentDirectoryLoader();

  @Override
  public SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext context)
      throws Exception {
    RemoteQueryConfigs configs = RemoteQueryConfigs.fromTableConfig(context.getTableConfig());
    if (configs == null) {
      return _defaultLoader.load(indexDir, context);
    }
    String segmentName = context.getSegmentName();
    URI segmentBaseUri = configs.segmentBaseUri(segmentName);
    File localScratchDir = new File(new File(context.getTableDataDir()), segmentName);
    RemoteSegmentMetadata metadata = RemoteSegmentRegistry.getInstance()
        .getOrRegister(context.getTableConfig().getTableName(), segmentName, context.getSegmentCrc(),
            segmentBaseUri, localScratchDir);
    LOGGER.info("Loaded remote segment: {} for table: {} from: {} (no data download)", segmentName,
        context.getTableConfig().getTableName(), segmentBaseUri);
    return new RemoteSegmentDirectory(metadata, RemoteIndexFetcher.getInstance());
  }

  @Override
  public void delete(SegmentDirectoryLoaderContext context)
      throws Exception {
    RemoteQueryConfigs configs = RemoteQueryConfigs.fromTableConfig(context.getTableConfig());
    if (configs == null) {
      _defaultLoader.delete(context);
      return;
    }
    RemoteSegmentRegistry.getInstance().remove(context.getTableConfig().getTableName(),
        context.getSegmentName());
  }
}
