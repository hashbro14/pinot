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
package org.apache.pinot.core.query.prefetch;

import java.util.Map;
import org.apache.pinot.segment.local.segment.store.RemoteIndexFetcher;
import org.apache.pinot.segment.local.segment.store.RemoteQueryConfigs;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wires remote (deep-store) querying into a server at startup: publishes the {@code pinot.server.instance.remote.*}
 * configuration as system properties (which is where the segment-local remote classes read it), sizes the shared
 * ranged-read fetcher from it, and installs the {@link RemoteFetchPlanner}.
 *
 * <p>Must run before the query executor is built: the fetch planner registry is frozen on first use.
 *
 * <p>Thread-safety: called once from the server starter.
 */
public final class RemoteQueryBootstrap {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteQueryBootstrap.class);
  private static final String REMOTE_KEY_PREFIX = "remote.";

  private RemoteQueryBootstrap() {
  }

  /**
   * @param instanceConfig the {@code pinot.server.instance} subset of the server configuration
   */
  public static void bootstrap(PinotConfiguration instanceConfig) {
    int bridged = 0;
    for (Map.Entry<String, Object> entry : instanceConfig.toMap().entrySet()) {
      if (!entry.getKey().startsWith(REMOTE_KEY_PREFIX) || entry.getValue() == null) {
        continue;
      }
      String property = RemoteQueryConfigs.INSTANCE_PROPERTY_PREFIX + entry.getKey();
      // An explicit -D on the command line wins over the config file
      if (System.getProperty(property) == null) {
        System.setProperty(property, String.valueOf(entry.getValue()));
        bridged++;
      }
    }
    RemoteIndexFetcher.init(RemoteQueryConfigs.fetchParallelism(), RemoteQueryConfigs.coalesceGapBytes(),
        RemoteQueryConfigs.fetchTimeoutSeconds());
    boolean plannerInstalled = FetchPlannerRegistry.registerPlanner(new RemoteFetchPlanner());
    LOGGER.info("Remote querying enabled: {} remote.* configs published, fetch parallelism: {}, plan prefetch: {}, "
            + "pin ceiling: {} bytes, per-query budget: {} bytes, storage mode: {}, fetch planner installed: {}",
        bridged, RemoteQueryConfigs.fetchParallelism(), RemoteQueryConfigs.planPrefetchEnabled(),
        RemoteQueryConfigs.pinMaxBytes(), RemoteQueryConfigs.maxFetchBytesPerQuery(),
        RemoteQueryConfigs.storageMode(), plannerInstalled);
  }
}
