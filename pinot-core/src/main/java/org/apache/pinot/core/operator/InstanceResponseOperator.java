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
package org.apache.pinot.core.operator;

import java.util.List;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.store.RemoteIndexFetcher;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String EXPLAIN_NAME = "INSTANCE_RESPONSE";
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceResponseOperator.class);

  protected final BaseCombineOperator<?> _combineOperator;
  protected final List<SegmentContext> _segmentContexts;
  protected final List<FetchContext> _fetchContexts;
  protected final int _fetchContextSize;
  protected final QueryContext _queryContext;

  protected long _threadCpuTimeNs;
  protected long _threadMemAllocatedBytes;
  protected long _systemActivitiesCpuTimeNs;

  // Snapshot of the process-wide remote fetch counters taken at prefetch time, to attribute GETs to this query
  private long _remoteFetchesBefore = -1;
  private long _remoteBytesBefore;
  private long _remoteFetchNanosBefore;
  private long _remoteStartNanos;

  public InstanceResponseOperator(BaseCombineOperator<?> combineOperator, List<SegmentContext> segmentContexts,
      List<FetchContext> fetchContexts, QueryContext queryContext) {
    _combineOperator = combineOperator;
    _segmentContexts = segmentContexts;
    _fetchContexts = fetchContexts;
    _fetchContextSize = fetchContexts.size();
    _queryContext = queryContext;
  }

  /*
   * Derive systemActivitiesCpuTimeNs from totalWallClockTimeNs, multipleThreadCpuTimeNs, mainThreadCpuTimeNs,
   * and numServerThreads.
   *
   * For example, let's divide query processing into 4 phases:
   * - phase 1: single thread (main thread) preparing. Time used: T1
   * - phase 2: N threads processing segments in parallel, each thread use time T2
   * - phase 3: system activities (GC/OS paging). Time used: T3
   * - phase 4: single thread (main thread) merging intermediate results blocks. Time used: T4
   *
   * Then we have following equations:
   * - mainThreadCpuTimeNs = T1 + T4
   * - multipleThreadCpuTimeNs = T2 * N
   * - totalWallClockTimeNs = T1 + T2 + T3 + T4 = mainThreadCpuTimeNs + T2 + T3
   * - systemActivitiesCpuTimeNs = T3 = totalWallClockTimeNs - mainThreadCpuTimeNs - T2
   */
  public static long calSystemActivitiesCpuTimeNs(long totalWallClockTimeNs, long multipleThreadCpuTimeNs,
      long mainThreadCpuTimeNs, int numServerThreads) {
    double perMultipleThreadCpuTimeNs = multipleThreadCpuTimeNs * 1.0 / numServerThreads;
    long systemActivitiesCpuTimeNs =
        Math.round(totalWallClockTimeNs - mainThreadCpuTimeNs - perMultipleThreadCpuTimeNs);
    // systemActivitiesCpuTimeNs should not be negative, this is just a defensive check
    return Math.max(systemActivitiesCpuTimeNs, 0);
  }

  /// Do not check termination in combine operator to ensure [#nextBlock()] returns a results block instead of
  /// throwing exception.
  @Override
  protected void checkTermination() {
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    return buildInstanceResponseBlock(getBaseBlock());
  }

  protected InstanceResponseBlock buildInstanceResponseBlock(BaseResultsBlock baseResultsBlock) {
    InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(baseResultsBlock);
    instanceResponseBlock.addMetadata(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(_threadCpuTimeNs));
    instanceResponseBlock.addMetadata(MetadataKey.THREAD_MEM_ALLOCATED_BYTES.getName(),
        String.valueOf(_threadMemAllocatedBytes));
    instanceResponseBlock.addMetadata(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(),
        String.valueOf(_systemActivitiesCpuTimeNs));
    Integer implicitLimit = QueryOptionsUtils.getLiteModeImplicitLeafStageLimit(
        _queryContext.getQueryOptions());
    // false-positive when table has exactly implicitLimit rows
    if (implicitLimit != null && baseResultsBlock.getNumRows() >= implicitLimit) {
      instanceResponseBlock.addMetadata(
          MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName(), "true");
    }
    return instanceResponseBlock;
  }

  protected BaseResultsBlock getBaseBlock() {
    long startWallClockTimeNs = System.nanoTime();
    ThreadResourceSnapshot resourceSnapshot = new ThreadResourceSnapshot();

    BaseResultsBlock resultsBlock = getCombinedResults();

    long mainThreadCpuTimeNs = resourceSnapshot.getCpuTimeNs();
    long mainThreadMemAllocatedBytes = resourceSnapshot.getAllocatedBytes();

    long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;

    calculateResourceUsage(resultsBlock.getNumServerThreads(), resultsBlock.getExecutionThreadCpuTimeNs(),
        mainThreadCpuTimeNs, resultsBlock.getExecutionThreadMemAllocatedBytes(), mainThreadMemAllocatedBytes,
        totalWallClockTimeNs);

    return resultsBlock;
  }

  private void calculateResourceUsage(int numServerThreads, long multipleThreadCpuTimeNs, long mainThreadCpuTimeNs,
      long multipleThreadMemAllocatedBytes, long mainThreadMemAllocatedBytes, long totalWallClockTimeNs) {
    _threadCpuTimeNs = mainThreadCpuTimeNs + multipleThreadCpuTimeNs;
    _threadMemAllocatedBytes = mainThreadMemAllocatedBytes + multipleThreadMemAllocatedBytes;
    _systemActivitiesCpuTimeNs = mainThreadCpuTimeNs == 0 ? 0
        : calSystemActivitiesCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, mainThreadCpuTimeNs,
            numServerThreads);
  }

  protected BaseResultsBlock getCombinedResults() {
    try {
      prefetchAll();
      // Combine operator should never throw exception
      return _combineOperator.nextBlock();
    } finally {
      releaseAll();
    }
  }

  public void prefetchAll() {
    RemoteIndexFetcher fetcher = RemoteIndexFetcher.peekInstance();
    if (fetcher != null) {
      _remoteFetchesBefore = fetcher.getFetchCount();
      _remoteBytesBefore = fetcher.getFetchedBytes();
      _remoteFetchNanosBefore = fetcher.getFetchNanos();
      _remoteStartNanos = System.nanoTime();
    }
    for (int i = 0; i < _fetchContextSize; i++) {
      _segmentContexts.get(i).getIndexSegment().prefetch(_fetchContexts.get(i));
    }
  }

  public void releaseAll() {
    for (int i = 0; i < _fetchContextSize; i++) {
      _segmentContexts.get(i).getIndexSegment().release(_fetchContexts.get(i));
    }
    logRemoteFetchStats();
  }

  /**
   * Logs, at DEBUG, the remote (deep-store) GETs this query caused; exact for one query at a time, indicative
   * under concurrency. Enable the logger for this class to see it.
   */
  private void logRemoteFetchStats() {
    RemoteIndexFetcher fetcher = RemoteIndexFetcher.peekInstance();
    if (fetcher == null || _remoteFetchesBefore < 0 || !LOGGER.isDebugEnabled()) {
      return;
    }
    long fetches = fetcher.getFetchCount() - _remoteFetchesBefore;
    if (fetches > 0) {
      long bytes = fetcher.getFetchedBytes() - _remoteBytesBefore;
      long fetchMs = (fetcher.getFetchNanos() - _remoteFetchNanosBefore) / 1_000_000;
      long wallMs = (System.nanoTime() - _remoteStartNanos) / 1_000_000;
      LOGGER.debug("Remote fetch stats for table: {}, {} segments: {} GETs, {} bytes, {} ms inside GETs, {} ms wall",
          _queryContext.getTableName(), _segmentContexts.size(), fetches, bytes, fetchMs, wallMs);
    }
    _remoteFetchesBefore = -1;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_combineOperator);
  }
}
