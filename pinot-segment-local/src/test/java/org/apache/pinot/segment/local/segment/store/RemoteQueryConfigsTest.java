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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * The instance-level keys here are stored without the {@code pinot.server.instance.} prefix, which the
 * lookup helpers add. A key that carries the prefix as well is never found and silently falls back to its
 * default — the setting appears to exist and does nothing. These assert each override actually lands.
 */
public class RemoteQueryConfigsTest {
  private static final String PREFIX = "pinot.server.instance.";

  @AfterMethod
  public void clearOverrides() {
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PREFETCH_COALESCE_GAP_BYTES);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PREFETCH_MAX_BYTES);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PLAN_PREFETCH_ENABLED);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PIN_MAX_BYTES);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_FETCH_DEBUG_LATENCY_MS);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_HINT_PROMOTE_MAX_BYTES);
    System.clearProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PIPELINED_HINTS_ENABLED);
  }

  @Test
  public void testHintedPromoteCeilingAndPipelinedHintsOverrides() {
    assertEquals(RemoteQueryConfigs.hintedPromoteMaxBytes(), RemoteQueryConfigs.DEFAULT_HINT_PROMOTE_MAX_BYTES);
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_HINT_PROMOTE_MAX_BYTES, "1024");
    assertEquals(RemoteQueryConfigs.hintedPromoteMaxBytes(), 1024L);
    assertTrue(RemoteQueryConfigs.pipelinedHintsEnabled());
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PIPELINED_HINTS_ENABLED, "false");
    assertFalse(RemoteQueryConfigs.pipelinedHintsEnabled());
  }

  @Test
  public void testPrefetchCoalesceGapOverrideIsHonoured() {
    assertEquals(RemoteQueryConfigs.prefetchCoalesceGapBytes(),
        RemoteQueryConfigs.DEFAULT_PREFETCH_COALESCE_GAP_BYTES);
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PREFETCH_COALESCE_GAP_BYTES, "4096");
    assertEquals(RemoteQueryConfigs.prefetchCoalesceGapBytes(), 4096L);
  }

  @Test
  public void testPrefetchMaxBytesOverrideIsHonoured() {
    assertEquals(RemoteQueryConfigs.prefetchMaxBytes(), RemoteQueryConfigs.DEFAULT_PREFETCH_MAX_BYTES);
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PREFETCH_MAX_BYTES, "1048576");
    assertEquals(RemoteQueryConfigs.prefetchMaxBytes(), 1048576L);
  }

  @Test
  public void testPlanPrefetchDefaultsOnAndCanBeDisabled() {
    assertTrue(RemoteQueryConfigs.planPrefetchEnabled());
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PLAN_PREFETCH_ENABLED, "false");
    assertFalse(RemoteQueryConfigs.planPrefetchEnabled());
  }

  @Test
  public void testPinCeilingDefaultsBelowPromoteCeilingAndIsCappedByIt() {
    assertEquals(RemoteQueryConfigs.pinMaxBytes(), RemoteQueryConfigs.DEFAULT_PIN_MAX_BYTES);
    assertTrue(RemoteQueryConfigs.pinMaxBytes() <= RemoteQueryConfigs.promoteMaxBytes());
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PIN_MAX_BYTES, "4194304");
    assertEquals(RemoteQueryConfigs.pinMaxBytes(), 4194304L);
    // Never above what a miss may promote: pinning more than that would bypass the promote ceiling
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_PIN_MAX_BYTES, String.valueOf(Long.MAX_VALUE));
    assertEquals(RemoteQueryConfigs.pinMaxBytes(), RemoteQueryConfigs.promoteMaxBytes());
  }

  @Test
  public void testDebugLatencyDefaultsToZero() {
    assertEquals(RemoteQueryConfigs.fetchDebugLatencyMs(), 0L);
    System.setProperty(PREFIX + RemoteQueryConfigs.INSTANCE_FETCH_DEBUG_LATENCY_MS, "25");
    assertEquals(RemoteQueryConfigs.fetchDebugLatencyMs(), 25L);
  }

  @Test
  public void testInstanceKeysDoNotCarryThePrefixThemselves() {
    for (String key : new String[]{
        RemoteQueryConfigs.INSTANCE_PREFETCH_COALESCE_GAP_BYTES, RemoteQueryConfigs.INSTANCE_PREFETCH_MAX_BYTES,
        RemoteQueryConfigs.INSTANCE_PLAN_PREFETCH_ENABLED, RemoteQueryConfigs.INSTANCE_PROMOTE_MAX_BYTES,
        RemoteQueryConfigs.INSTANCE_PIN_MAX_BYTES, RemoteQueryConfigs.INSTANCE_FETCH_DEBUG_LATENCY_MS,
        RemoteQueryConfigs.INSTANCE_HINT_PROMOTE_MAX_BYTES, RemoteQueryConfigs.INSTANCE_PIPELINED_HINTS_ENABLED,
        RemoteQueryConfigs.INSTANCE_MAX_FETCH_BYTES_PER_QUERY, RemoteQueryConfigs.INSTANCE_COALESCE_GAP_BYTES,
        RemoteQueryConfigs.INSTANCE_FETCH_PARALLELISM, RemoteQueryConfigs.INSTANCE_FETCH_TIMEOUT_SECONDS,
        RemoteQueryConfigs.INSTANCE_STORAGE_MODE, RemoteQueryConfigs.INSTANCE_CACHE_ENABLED}) {
      assertFalse(key.startsWith(PREFIX), "key must be stored unprefixed, the lookup adds it: " + key);
    }
  }
}
