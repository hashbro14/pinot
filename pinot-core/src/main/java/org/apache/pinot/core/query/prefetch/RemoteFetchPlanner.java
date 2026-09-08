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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * Fetch planner for segments whose data lives in the deep store: decides, per column, which index entries a
 * query is going to need so the remote segment directory can start fetching them before execution begins.
 *
 * <p>The {@link DefaultFetchPlanner} asks for every index of every column the query mentions. Against object
 * storage that is either far too much (a ten-row lookup does not want the forward index of every projected
 * column) or, once the directory declines it for size, nothing at all. This planner asks for exactly what sits
 * on the query's critical path:
 * <ul>
 *   <li>Filter columns: the dictionary (predicate values are resolved by binary search — a chain of dependent
 *   reads), plus whatever filter index exists (inverted, range, bloom), plus the forward index when the column
 *   is sorted (the sorted index <em>is</em> the forward index) or has no filter index at all (a scan reads it
 *   end to end).</li>
 *   <li>Columns read for every matching row — aggregation inputs, group-by keys, order-by keys: dictionary and
 *   forward index.</li>
 *   <li>Columns only projected for the final rows of a selection: dictionary only. The forward index is read
 *   for a handful of docIds, which the block-level hints fetch precisely.</li>
 *   <li>Aggregations the segment's star-tree can serve: dictionaries of filter and group-by columns only. The
 *   tree's own buffers are local, and the raw forward indexes are never touched.</li>
 * </ul>
 * Entry sizes are not known here; the directory applies its pin ceiling and per-query budget to the result.
 * Pruning keeps the default behaviour (bloom filters of EQ/IN columns).
 *
 * <p>Thread-safety: stateless.
 */
public class RemoteFetchPlanner extends DefaultFetchPlanner {
  /** Indexes worth having local for a column the filter touches. Missing ones are skipped by the directory. */
  private static final List<IndexType<?, ?, ?>> FILTER_INDEXES =
      List.of(StandardIndexes.dictionary(), StandardIndexes.inverted(), StandardIndexes.range(),
          StandardIndexes.bloomFilter());
  private static final List<IndexType<?, ?, ?>> FILTER_INDEXES_WITH_FORWARD =
      List.of(StandardIndexes.dictionary(), StandardIndexes.inverted(), StandardIndexes.range(),
          StandardIndexes.bloomFilter(), StandardIndexes.forward());
  private static final List<IndexType<?, ?, ?>> DICTIONARY_ONLY = List.of(StandardIndexes.dictionary());
  private static final List<IndexType<?, ?, ?>> DICTIONARY_AND_FORWARD =
      List.of(StandardIndexes.dictionary(), StandardIndexes.forward());

  @Override
  public FetchContext planFetchForProcessing(IndexSegment indexSegment, QueryContext queryContext) {
    Map<String, List<IndexType<?, ?, ?>>> plan = new HashMap<>();

    Set<String> filterColumns = new HashSet<>();
    FilterContext filter = queryContext.getFilter();
    if (filter != null) {
      filter.getColumns(filterColumns);
    }
    Set<String> groupByColumns = new HashSet<>();
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      for (ExpressionContext expression : groupByExpressions) {
        expression.getColumns(groupByColumns);
      }
    }

    // Star-tree served: only the dictionaries needed to resolve predicate values and decode group keys
    if (servedByStarTree(indexSegment, queryContext, filterColumns, groupByExpressions)) {
      for (String column : filterColumns) {
        plan.put(column, DICTIONARY_ONLY);
      }
      for (String column : groupByColumns) {
        plan.put(column, DICTIONARY_ONLY);
      }
      return new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), plan);
    }

    // Columns read for every matching row
    Set<String> scannedColumns = new HashSet<>(groupByColumns);
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    if (aggregationFunctions != null) {
      for (AggregationFunction<?, ?> function : aggregationFunctions) {
        for (ExpressionContext expression : function.getInputExpressions()) {
          expression.getColumns(scannedColumns);
        }
      }
    }
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions != null) {
      for (OrderByExpressionContext orderBy : orderByExpressions) {
        orderBy.getColumns(scannedColumns);
      }
    }
    if (QueryContextUtils.isDistinctQuery(queryContext)) {
      for (ExpressionContext expression : queryContext.getSelectExpressions()) {
        expression.getColumns(scannedColumns);
      }
    }
    for (String column : scannedColumns) {
      plan.put(column, DICTIONARY_AND_FORWARD);
    }

    // Columns only decoded for the rows a selection returns
    for (String column : selectedColumns(indexSegment, queryContext)) {
      plan.putIfAbsent(column, DICTIONARY_ONLY);
    }

    // Filter columns last, so their (larger) index list wins when a column plays several roles
    for (String column : filterColumns) {
      boolean forward = plan.get(column) == DICTIONARY_AND_FORWARD || needsForwardScan(indexSegment, column);
      plan.put(column, forward ? FILTER_INDEXES_WITH_FORWARD : FILTER_INDEXES);
    }
    return new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), plan);
  }

  /** Columns in the select list; every physical column for {@code SELECT *}. */
  private static Set<String> selectedColumns(IndexSegment indexSegment, QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    if (selectExpressions.size() == 1 && "*".equals(selectExpressions.get(0).getIdentifier())) {
      return indexSegment.getPhysicalColumnNames();
    }
    Set<String> columns = new HashSet<>();
    for (ExpressionContext expression : selectExpressions) {
      expression.getColumns(columns);
    }
    return columns;
  }

  /**
   * True when evaluating a predicate on the column means reading its forward index end to end: the column is
   * sorted (its sorted index is the forward index) or has no inverted/range index to answer from.
   */
  private static boolean needsForwardScan(IndexSegment indexSegment, String column) {
    DataSource dataSource = indexSegment.getDataSourceNullable(column);
    if (dataSource == null) {
      return false;
    }
    if (dataSource.getDataSourceMetadata().isSorted()) {
      return true;
    }
    return dataSource.getInvertedIndex() == null && dataSource.getRangeIndex() == null;
  }

  /**
   * Mirrors the star-tree eligibility check of the aggregation plan: every aggregation is a function-column
   * pair the tree pre-aggregates, and every filter and group-by column is a tree dimension.
   */
  private static boolean servedByStarTree(IndexSegment indexSegment, QueryContext queryContext,
      Set<String> filterColumns, @Nullable List<ExpressionContext> groupByExpressions) {
    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    if (starTrees == null || starTrees.isEmpty() || aggregationFunctions == null || queryContext.isSkipStarTree()) {
      return false;
    }
    List<Pair<AggregationFunction, FilterContext>> filteredAggregations =
        queryContext.getFilteredAggregationFunctions();
    if (filteredAggregations != null) {
      for (Pair<AggregationFunction, FilterContext> filtered : filteredAggregations) {
        if (filtered.getRight() != null) {
          // FILTER(WHERE ...) aggregations are planned per filter; keep the conservative scan plan for them
          return false;
        }
      }
    }
    AggregationFunctionColumnPair[] pairs = StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
    if (pairs == null) {
      return false;
    }
    List<Pair<AggregationFunction, AggregationFunctionColumnPair>> aggregations = new ArrayList<>(pairs.length);
    for (int i = 0; i < pairs.length; i++) {
      aggregations.add(Pair.of(aggregationFunctions[i], pairs[i]));
    }
    ExpressionContext[] groupBy =
        groupByExpressions != null ? groupByExpressions.toArray(new ExpressionContext[0]) : null;
    for (StarTreeV2 starTree : starTrees) {
      if (StarTreeUtils.isFitForStarTree(starTree.getMetadata(), aggregations, groupBy, filterColumns)) {
        return true;
      }
    }
    return false;
  }
}
