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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RemoteFetchPlannerTest {
  private static final String TABLE = "testTable";

  private final RemoteFetchPlanner _planner = new RemoteFetchPlanner();

  private static IndexSegment segment(Set<String> columns, Set<String> sortedColumns, Set<String> invertedColumns) {
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getSegmentName()).thenReturn("s0");
    when(segment.getColumnNames()).thenReturn(columns);
    when(segment.getPhysicalColumnNames()).thenReturn(columns);
    for (String column : columns) {
      DataSource dataSource = mock(DataSource.class);
      DataSourceMetadata metadata = mock(DataSourceMetadata.class);
      when(metadata.isSorted()).thenReturn(sortedColumns.contains(column));
      when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
      when(dataSource.getInvertedIndex()).thenReturn(
          invertedColumns.contains(column) ? mock(InvertedIndexReader.class) : null);
      when(segment.getDataSourceNullable(column)).thenReturn(dataSource);
      when(segment.getDataSource(column)).thenReturn(dataSource);
    }
    return segment;
  }

  private static Map<String, List<IndexType<?, ?, ?>>> plan(RemoteFetchPlanner planner, IndexSegment segment,
      String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    FetchContext fetchContext = planner.planFetchForProcessing(segment, queryContext);
    assertEquals(fetchContext.getSegmentName(), "s0");
    return fetchContext.getColumnToIndexList();
  }

  @Test
  public void testSelectionWithLimitPinsDictionariesOnly() {
    IndexSegment segment = segment(Set.of("a", "b", "c"), Set.of(), Set.of());
    Map<String, List<IndexType<?, ?, ?>>> plan = plan(_planner, segment, "SELECT * FROM " + TABLE + " LIMIT 10");
    assertEquals(plan.keySet(), Set.of("a", "b", "c"));
    for (List<IndexType<?, ?, ?>> indexes : plan.values()) {
      // The forward index of a projected column is fetched block by block, never whole
      assertEquals(indexes, List.of(StandardIndexes.dictionary()));
    }
  }

  @Test
  public void testFilterColumnGetsFilterIndexesAndScanForwardWhenUnindexed() {
    IndexSegment segment = segment(Set.of("a", "b", "sorted", "inv"), Set.of("sorted"), Set.of("inv"));
    Map<String, List<IndexType<?, ?, ?>>> plan = plan(_planner, segment,
        "SELECT a FROM " + TABLE + " WHERE b = 1 AND sorted = 2 AND inv = 3 LIMIT 10");
    assertEquals(plan.get("a"), List.of(StandardIndexes.dictionary()));
    // Unindexed predicate: the filter scans the forward index end to end
    assertTrue(plan.get("b").contains(StandardIndexes.forward()));
    assertTrue(plan.get("b").contains(StandardIndexes.dictionary()));
    // Sorted column: the sorted index is the forward index
    assertTrue(plan.get("sorted").contains(StandardIndexes.forward()));
    // Inverted index answers the predicate; the forward index stays remote
    assertFalse(plan.get("inv").contains(StandardIndexes.forward()));
    assertTrue(plan.get("inv").contains(StandardIndexes.inverted()));
    assertTrue(plan.get("inv").contains(StandardIndexes.dictionary()));
  }

  @Test
  public void testAggregationScanPinsForwardIndexes() {
    IndexSegment segment = segment(Set.of("a", "m", "k"), Set.of(), Set.of());
    Map<String, List<IndexType<?, ?, ?>>> plan = plan(_planner, segment,
        "SELECT k, SUM(m) FROM " + TABLE + " WHERE a > 5 GROUP BY k ORDER BY SUM(m) DESC LIMIT 10");
    assertEquals(plan.get("m"), List.of(StandardIndexes.dictionary(), StandardIndexes.forward()));
    assertEquals(plan.get("k"), List.of(StandardIndexes.dictionary(), StandardIndexes.forward()));
    assertTrue(plan.get("a").contains(StandardIndexes.forward()));
  }

  @Test
  public void testStarTreeServedAggregationPinsDictionariesOnly() {
    IndexSegment segment = segment(Set.of("d1", "d2", "m"), Set.of(), Set.of());
    StarTreeV2Metadata metadata = mock(StarTreeV2Metadata.class);
    when(metadata.getDimensionsSplitOrder()).thenReturn(List.of("d1", "d2"));
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> specs = new TreeMap<>();
    specs.put(AggregationFunctionColumnPair.fromColumnName("sum__m"), AggregationSpec.DEFAULT);
    when(metadata.getAggregationSpecs()).thenReturn(specs);
    StarTreeV2 starTree = mock(StarTreeV2.class);
    when(starTree.getMetadata()).thenReturn(metadata);
    when(segment.getStarTrees()).thenReturn(List.of(starTree));

    Map<String, List<IndexType<?, ?, ?>>> plan =
        plan(_planner, segment, "SELECT d2, SUM(m) FROM " + TABLE + " WHERE d1 = 'x' GROUP BY d2");
    // The tree answers the aggregation: the metric's raw forward index is never read
    assertNull(plan.get("m"));
    assertEquals(plan.get("d1"), List.of(StandardIndexes.dictionary()));
    assertEquals(plan.get("d2"), List.of(StandardIndexes.dictionary()));

    // A predicate outside the tree's dimensions disqualifies it: back to the scan plan
    plan = plan(_planner, segment, "SELECT d2, SUM(m) FROM " + TABLE + " WHERE m > 3 GROUP BY d2");
    assertTrue(plan.get("m").contains(StandardIndexes.forward()));
  }

  @Test
  public void testPruningKeepsDefaultBehaviour() {
    IndexSegment segment = segment(Set.of("a"), Set.of(), Set.of());
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM " + TABLE + " WHERE a = 1");
    FetchContext fetchContext = _planner.planFetchForPruning(segment, queryContext);
    // No bloom filter on the mocked column: nothing to fetch for pruning
    assertTrue(fetchContext.isEmpty());
  }
}
