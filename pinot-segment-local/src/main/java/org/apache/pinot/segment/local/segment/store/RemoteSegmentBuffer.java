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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotDataBuffer} over one index entry of a segment that lives in the deep store.
 *
 * <p>Reads are served in tiers: from the entry when the directory holds it resident (pinned by plan-time
 * prefetch or promoted whole), from ranges already fetched into this buffer, from ranges a batch hint put in
 * flight, and only then by a fresh ranged read with read-ahead sized to the access pattern.
 *
 * <p>Batch hints ({@link #prefetchRanges}) are asynchronous: the caller is not made to wait for bytes it has
 * not asked to read yet, so an operator can hint every column of a block and then read them, paying one round
 * trip for the block instead of one per column. A read that lands in a range still in flight waits for that
 * range rather than fetching around it.
 *
 * <p>Thread-safety: the sub-range and pending-range state is guarded by the sub-range map's monitor; resident
 * views are re-resolved per call so the directory may free entries at any time.
 */
public class RemoteSegmentBuffer extends PinotDataBuffer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSegmentBuffer.class);
  /** Cap for the sub-range cache held per buffer. */
  private static final long MAX_CACHED_SUB_RANGE_BYTES = 64L << 20;
  /** Small reads are rounded up to this granularity to spare repeated header fetches. */
  private static final int MIN_FETCH_BYTES = 4096;
  /**
   * Read-ahead for a read that continues where the previous one stopped. Scans (group-by, full-column
   * aggregation) walk an entry front to back, so pulling ahead in large chunks turns thousands of requests
   * into a handful.
   */
  private static final int SEQUENTIAL_READ_AHEAD_BYTES = 1 << 20;
  /**
   * Read-ahead for a read that jumps somewhere else in the entry. Random access is what dictionary lookups
   * do — resolving a value for one docId touches an offset slot and a few dozen bytes of payload — so reading
   * ahead by hundreds of KB per lookup transfers orders of magnitude more than the query needs.
   */
  private static final int RANDOM_READ_AHEAD_BYTES = 16 << 10;
  /** A read starting within this distance of the previous one still counts as sequential. */
  private static final long SEQUENTIAL_GAP_BYTES = 64 << 10;
  /**
   * After this many random reads into the same entry, stop serving it range by range and materialize it in
   * one streamed fetch. Scattered dictionary lookups are request-bound, not bandwidth-bound: a thousand
   * round trips cost far more than one bulk transfer, and in spill mode the materialized entry is mapped
   * from disk, so this costs no heap.
   */
  private static final int RANDOM_READS_BEFORE_MATERIALIZE = 96;
  /** Layout of a variable-length dictionary (see VarLengthValueWriter): magic, version, count, data start. */
  private static final byte[] VAR_LENGTH_MAGIC = {'.', 'v', 'l', ';'};
  private static final int VAR_LENGTH_NUM_VALUES_OFFSET = 8;
  private static final int VAR_LENGTH_DATA_SECTION_OFFSET_POSITION = 12;
  static final int VAR_LENGTH_HEADER_BYTES = 16;
  /** Refuse to prime absurdly large offset tables; those fall back to ordinary ranged reads. */
  private static final long MAX_OFFSET_TABLE_BYTES = 16L << 20;
  /** Two prefetched ranges closer than this are merged: one bigger GET beats two with a hole between them. */
  private static final long PREFETCH_COALESCE_GAP_BYTES = RemoteQueryConfigs.prefetchCoalesceGapBytes();
  /** Give up on prefetching rather than blow the sub-range cache. */
  private static final long MAX_PREFETCH_BYTES = RemoteQueryConfigs.prefetchMaxBytes();
  /** Prefetching is declined once a batch spans more than 1/this of the entry. */
  private static final int WHOLE_ENTRY_PREFETCH_RATIO = 4;
  /** Hard ceiling on requests issued for one batch; past it, reading the entry whole is cheaper. */
  private static final int MAX_PREFETCH_RANGES = RemoteQueryConfigs.prefetchMaxRanges();
  /** Largest entry a declined (too scattered) hint may materialize whole instead. */
  private static final long HINTED_PROMOTE_MAX_BYTES = RemoteQueryConfigs.hintedPromoteMaxBytes();

  private final RemoteSegmentDirectory _directory;
  private final IndexKey _key;
  /** Offset of this buffer within the index entry's data (0 for the whole entry, > 0 for views). */
  private final long _baseOffset;
  private final long _size;
  private final ByteOrder _order;

  /** Identity-tracked resident entry and the view of it matching this buffer's range/order. */
  private volatile PinotDataBuffer _cachedResident;
  private volatile PinotDataBuffer _cachedResidentView;

  /** Load-mode cache: start offset (within this buffer) -> fetched bytes. Guarded by itself. */
  private final NavigableMap<Long, byte[]> _subRanges = new TreeMap<>();
  private long _cachedSubRangeBytes;
  /** Ranges a batch hint has put in flight and that are not yet in {@link #_subRanges}. Guarded by _subRanges. */
  private final List<PendingRange> _pending = new ArrayList<>();
  /** End of the previous ranged read, used to tell a scan apart from random lookups. */
  private volatile long _lastReadEnd = -1;
  /** True when this entry is a dictionary, whose offset table is worth fetching up front. */
  private final boolean _dictionary;
  /** Ranged reads this buffer has actually issued; used by tests and for diagnosing fetch patterns. */
  private final AtomicLong _rangeFetches = new AtomicLong();
  /** Random (non-sequential) reads served so far; drives escalation to a single bulk fetch. */
  private final AtomicInteger _randomReads = new AtomicInteger();

  /** A hinted range whose bytes are on the way; {@code _done} completes once they are in the sub-range cache. */
  private static final class PendingRange {
    final long _start;
    final long _end;
    final CompletableFuture<Void> _done;

    PendingRange(long start, long end, CompletableFuture<Void> done) {
      _start = start;
      _end = end;
      _done = done;
    }

    boolean covers(long offset, int length) {
      return _start <= offset && offset + length <= _end;
    }
  }

  RemoteSegmentBuffer(RemoteSegmentDirectory directory, IndexKey key, long baseOffset, long size, ByteOrder order) {
    this(directory, key, baseOffset, size, order, false);
  }

  RemoteSegmentBuffer(RemoteSegmentDirectory directory, IndexKey key, long baseOffset, long size, ByteOrder order,
      boolean dictionary) {
    super(false);
    _directory = directory;
    _key = key;
    _baseOffset = baseOffset;
    _size = size;
    _order = order;
    _dictionary = dictionary;
  }

  /**
   * Parses the header of a dictionary entry. Returns {offset table start, offset table bytes} for a var-length
   * dictionary whose table is worth priming, {-1, -1} for a fixed-width dictionary or an unreasonable table.
   */
  static int[] parseVarLengthHeader(byte[] header, long entrySize) {
    for (int i = 0; i < VAR_LENGTH_MAGIC.length; i++) {
      if (header[i] != VAR_LENGTH_MAGIC[i]) {
        return new int[]{-1, -1}; // fixed-width dictionary: nothing to prime
      }
    }
    ByteBuffer head = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
    int numValues = head.getInt(VAR_LENGTH_NUM_VALUES_OFFSET);
    int offsetTableStart = head.getInt(VAR_LENGTH_DATA_SECTION_OFFSET_POSITION);
    long offsetTableBytes = (numValues + 1L) * Integer.BYTES;
    if (numValues <= 0 || offsetTableStart < 0 || offsetTableStart + offsetTableBytes > entrySize
        || offsetTableBytes > MAX_OFFSET_TABLE_BYTES) {
      return new int[]{-1, -1};
    }
    return new int[]{offsetTableStart, Math.toIntExact(offsetTableBytes)};
  }

  /**
   * Serves a read of a variable-length dictionary out of its offset table when the table is local and covers
   * the range; null otherwise.
   *
   * <p>Resolving one value of such a dictionary is three dependent reads: two into the offset table to learn
   * where the value lives, then the value itself. Locally those are page-cache hits; against object storage
   * each is a round trip, so a thousand-row projection becomes thousands of requests. The offset table is
   * contiguous ((numValues + 1) * 4 bytes), so having it once turns every later lookup into a single read
   * for the value bytes alone. The table is owned by the directory (materialized through the disk cache,
   * usually already in flight from plan-time prefetch) and re-resolved per call so directory frees are
   * honored. Fixed-width dictionaries have no table: their value positions are arithmetic.
   */
  @Nullable
  private ByteBuffer readFromOffsetTable(long offset, int length) {
    if (_baseOffset != 0) {
      return null; // a view into a dictionary: the table's positions would not line up
    }
    PinotDataBuffer table = _directory.offsetTable(_key);
    if (table == null) {
      return null;
    }
    int start = _directory.offsetTableStart(_key);
    if (offset < start || offset + length > start + table.size()) {
      return null;
    }
    byte[] bytes = new byte[length];
    table.copyTo(offset - start, bytes, 0, length);
    return ByteBuffer.wrap(bytes).order(_order);
  }

  /**
   * Returns a view of the resident entry matching this buffer's range and order, waiting for an in-flight
   * fetch of the entry, or null when the entry is not resident. Re-checked by identity per call so directory
   * frees are honored.
   */
  @Nullable
  private PinotDataBuffer residentView() {
    PinotDataBuffer resident = _directory.awaitResident(_key);
    if (resident == null) {
      _cachedResident = null;
      _cachedResidentView = null;
      return null;
    }
    if (resident != _cachedResident) {
      _cachedResidentView = (_baseOffset == 0 && _size == resident.size() && _order == resident.order())
          ? resident : resident.view(_baseOffset, _baseOffset + _size, _order);
      _cachedResident = resident;
    }
    return _cachedResidentView;
  }

  /** Number of ranged reads this buffer has issued since it was created. */
  long getRangeFetchCount() {
    return _rangeFetches.get();
  }

  private void cacheSubRange(long offset, byte[] fetched) {
    synchronized (_subRanges) {
      if (_subRanges.containsKey(offset)) {
        return;
      }
      // Bounded cache: drop the lowest-offset ranges first (scans move forward)
      while (_cachedSubRangeBytes + fetched.length > MAX_CACHED_SUB_RANGE_BYTES && !_subRanges.isEmpty()) {
        Map.Entry<Long, byte[]> oldest = _subRanges.pollFirstEntry();
        if (oldest == null) {
          break;
        }
        _cachedSubRangeBytes -= oldest.getValue().length;
      }
      _subRanges.put(offset, fetched);
      _cachedSubRangeBytes += fetched.length;
    }
  }

  /** Serves a read from the sub-range cache, or null when no cached range covers it. Caller holds the monitor. */
  @Nullable
  private ByteBuffer readCachedLocked(long offset, int length) {
    Map.Entry<Long, byte[]> floor = _subRanges.floorEntry(offset);
    if (floor != null && floor.getKey() + floor.getValue().length >= offset + length) {
      return ByteBuffer.wrap(floor.getValue(), Math.toIntExact(offset - floor.getKey()), length).order(_order);
    }
    return null;
  }

  /** Returns the in-flight hinted range covering the read, or null. Caller holds the monitor. */
  @Nullable
  private PendingRange pendingCoveringLocked(long offset, int length) {
    for (PendingRange pending : _pending) {
      if (pending.covers(offset, length)) {
        return pending;
      }
    }
    return null;
  }

  private boolean promotableSize() {
    return _size <= _directory.getMaxPromoteBytes();
  }

  private PinotDataBuffer promoteView() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("PROMOTE-WHOLE-ENTRY key={} size={} baseOffset={}", _key, _size, _baseOffset);
    }
    PinotDataBuffer resident = _directory.promoteSync(_key);
    PinotDataBuffer view = (_baseOffset == 0 && _size == resident.size() && _order == resident.order())
        ? resident : resident.view(_baseOffset, _baseOffset + _size, _order);
    _cachedResident = resident;
    _cachedResidentView = view;
    return view;
  }

  /** Serves a read through the tiered strategy, returning a ByteBuffer in this buffer's byte order. */
  private ByteBuffer readSlow(long offset, int length) {
    if (offset < 0 || offset + length > _size) {
      throw new IndexOutOfBoundsException(
          "Read [" + offset + ", +" + length + ") out of bounds for entry " + _key + " of size " + _size);
    }
    // 0. Dictionary offset table: materialized once (usually at plan time), then served locally
    if (_dictionary) {
      ByteBuffer fromTable = readFromOffsetTable(offset, length);
      if (fromTable != null) {
        return fromTable;
      }
    }
    // 1. Cached sub-range, or a hinted range still in flight: wait for it rather than fetch around it
    PendingRange pending;
    synchronized (_subRanges) {
      ByteBuffer cached = readCachedLocked(offset, length);
      if (cached != null) {
        return cached;
      }
      pending = pendingCoveringLocked(offset, length);
    }
    if (pending != null) {
      try {
        pending._done.join();
      } catch (RuntimeException e) {
        LOGGER.warn("Hinted fetch of {} failed for segment: {}; reading the range directly", _key,
            _directory.getRemoteMetadata().getSegmentName(), e);
      }
      synchronized (_subRanges) {
        ByteBuffer cached = readCachedLocked(offset, length);
        if (cached != null) {
          return cached;
        }
      }
    }
    // 2. Promote the whole entry only when it is small enough to be worth holding; a query-mode miss
    //    on a multi-GB entry must NOT materialize it (that is how a SELECT * OOMs the server).
    boolean promotable = promotableSize();
    if (promotable && (_directory.inQueryMode() || length > MAX_CACHED_SUB_RANGE_BYTES)) {
      PinotDataBuffer view = promoteView();
      byte[] bytes = new byte[length];
      view.copyTo(offset, bytes, 0, length);
      return ByteBuffer.wrap(bytes).order(_order);
    }
    if (!promotable && length > MAX_CACHED_SUB_RANGE_BYTES) {
      // Single read larger than the cache: serve it directly, uncached
      byte[] bytes = new byte[length];
      _rangeFetches.incrementAndGet();
      _directory.readDataRange(_key, _baseOffset + offset, bytes, length);
      return ByteBuffer.wrap(bytes).order(_order);
    }
    // 3. Ranged sub-read, with read-ahead chosen from the access pattern: a scan pulls ahead generously,
    //    a random lookup (the dictionary case) fetches barely more than it needs.
    int readAhead;
    if (promotable) {
      readAhead = MIN_FETCH_BYTES;
    } else {
      long lastEnd = _lastReadEnd;
      boolean sequential = lastEnd >= 0 && offset >= lastEnd && offset - lastEnd <= SEQUENTIAL_GAP_BYTES;
      if (!sequential && !wantsPrefetch() && _randomReads.incrementAndGet() > RANDOM_READS_BEFORE_MATERIALIZE) {
        // Walking the entry at random with no batch hint to work from (so the reads cannot be coalesced):
        // one bulk fetch beats hundreds more round trips. Entries that do get batch hints are excluded —
        // materializing them would throw away the coalescing and pull the whole entry instead.
        PinotDataBuffer view = promoteView();
        byte[] bytes = new byte[length];
        view.copyTo(offset, bytes, 0, length);
        return ByteBuffer.wrap(bytes).order(_order);
      }
      readAhead = sequential ? SEQUENTIAL_READ_AHEAD_BYTES : RANDOM_READ_AHEAD_BYTES;
    }
    int fetchLength = Math.toIntExact(Math.min(Math.max(length, readAhead), _size - offset));
    _lastReadEnd = offset + fetchLength;
    byte[] fetched = new byte[fetchLength];
    _rangeFetches.incrementAndGet();
    _directory.readDataRange(_key, _baseOffset + offset, fetched, fetchLength);
    cacheSubRange(offset, fetched);
    return ByteBuffer.wrap(fetched, 0, length).order(_order);
  }

  @Override
  public boolean wantsPrefetch() {
    if (_cachedResident != null || _directory.hasResident(_key)) {
      // Already materialized locally, or on its way whole (plan-time pin): reads are memory-speed once it lands,
      // and hinting ranges of it would only add requests for bytes already in flight
      return false;
    }
    return true;
  }

  @Override
  public boolean remoteBacked() {
    return true;
  }

  /**
   * Starts fetching the hinted ranges, merged into a bounded number of requests, and returns at once. Ranges
   * already cached or already in flight are skipped; the reads that follow wait for exactly the range they
   * need (see {@link #readSlow}). Declines — leaving the promote-on-miss path to pull the entry whole — when
   * the batch is so scattered or so large that reading the entry in one go is the cheaper plan.
   */
  @Override
  public void prefetchRanges(long[] offsets, int[] lengths, int count) {
    if (count <= 0) {
      return;
    }
    long[][] sorted = new long[count][];
    for (int i = 0; i < count; i++) {
      sorted[i] = new long[]{offsets[i], offsets[i] + lengths[i]};
    }
    // Sort by offset so neighbouring values merge; dictIds arrive in row order, which is random here.
    Arrays.sort(sorted, Comparator.comparingLong(r -> r[0]));

    // Merge, widening the gap until the batch fits in a bounded number of requests. Capping only the
    // bytes is not enough: a batch scattered across an entry stays well under the byte ceiling while
    // still needing roughly one request per value, and at a few hundred values per block, per column,
    // per segment that is thousands of round trips for one query.
    long gap = PREFETCH_COALESCE_GAP_BYTES;
    List<long[]> merged;
    long total;
    while (true) {
      merged = new ArrayList<>();
      total = 0;
      long rangeStart = -1;
      long rangeEnd = -1;
      for (long[] value : sorted) {
        long start = value[0];
        long end = value[1];
        if (rangeStart < 0) {
          rangeStart = start;
          rangeEnd = end;
        } else if (start <= rangeEnd + gap) {
          rangeEnd = Math.max(rangeEnd, end);
        } else {
          total += rangeEnd - rangeStart;
          merged.add(new long[]{rangeStart, rangeEnd});
          rangeStart = start;
          rangeEnd = end;
        }
      }
      if (rangeStart >= 0) {
        total += rangeEnd - rangeStart;
        merged.add(new long[]{rangeStart, rangeEnd});
      }
      if (merged.size() <= MAX_PREFETCH_RANGES || gap >= _size) {
        break;
      }
      gap *= 2;
    }
    if (total > MAX_PREFETCH_BYTES || total * WHOLE_ENTRY_PREFETCH_RATIO > _size
        || merged.size() > MAX_PREFETCH_RANGES) {
      // Reading the entry whole is now the cheaper plan (a full scan looks like this, and so does a batch too
      // scattered to merge). For an entry a miss may promote, decline and let the promote-on-miss path pull it
      // in one bounded read. For a bigger one — a high-cardinality string dictionary being resolved for
      // thousands of group keys — start the whole-entry fetch now: the alternative measured as one 16 KB round
      // trip per value, which no amount of parallelism turns into a sub-second query.
      if (!promotableSize() && _baseOffset == 0 && _size <= HINTED_PROMOTE_MAX_BYTES) {
        _directory.promoteAsync(_key);
      }
      return;
    }
    List<long[]> toFetch = new ArrayList<>(merged.size());
    List<byte[]> targets = new ArrayList<>(merged.size());
    CompletableFuture<Void> done = new CompletableFuture<>();
    List<PendingRange> registered = new ArrayList<>(merged.size());
    synchronized (_subRanges) {
      PinotDataBuffer offsetTable = _dictionary && _baseOffset == 0 ? _directory.peekOffsetTable(_key) : null;
      long offsetTableStart = offsetTable != null ? _directory.offsetTableStart(_key) : -1;
      for (long[] range : merged) {
        long start = range[0];
        int len = Math.toIntExact(Math.min(range[1] - start, _size - start));
        if (len <= 0 || readCachedLocked(start, len) != null || pendingCoveringLocked(start, len) != null) {
          continue;
        }
        if (offsetTable != null && start >= offsetTableStart && start + len <= offsetTableStart + offsetTable.size()) {
          continue; // served from the primed offset table, nothing to fetch
        }
        // element 0 is the offset to read, element 1 the key this range is cached under
        toFetch.add(new long[]{_baseOffset + start, start});
        targets.add(new byte[len]);
        PendingRange pending = new PendingRange(start, start + len, done);
        registered.add(pending);
        _pending.add(pending);
      }
    }
    if (toFetch.isEmpty()) {
      return;
    }
    // One call, so the batch's ranges are all in flight together rather than each waiting its turn — and off
    // this thread, so the caller can go on to hint its other columns before anything is waited for.
    _rangeFetches.addAndGet(toFetch.size());
    _directory.readDataRangesAsync(_key, toFetch, targets).whenComplete((ignored, error) -> {
      if (error == null) {
        for (int i = 0; i < toFetch.size(); i++) {
          cacheSubRange(toFetch.get(i)[1], targets.get(i));
        }
      }
      synchronized (_subRanges) {
        _pending.removeAll(registered);
      }
      if (error == null) {
        done.complete(null);
      } else {
        done.completeExceptionally(error);
      }
    });
  }

  @Override
  public byte getByte(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getByte(offset) : readSlow(offset, Byte.BYTES).get();
  }

  @Override
  public char getChar(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getChar(offset) : readSlow(offset, Character.BYTES).getChar();
  }

  @Override
  public short getShort(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getShort(offset) : readSlow(offset, Short.BYTES).getShort();
  }

  @Override
  public int getInt(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getInt(offset) : readSlow(offset, Integer.BYTES).getInt();
  }

  @Override
  public long getLong(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getLong(offset) : readSlow(offset, Long.BYTES).getLong();
  }

  @Override
  public float getFloat(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getFloat(offset) : readSlow(offset, Float.BYTES).getFloat();
  }

  @Override
  public double getDouble(long offset) {
    PinotDataBuffer resident = residentView();
    return resident != null ? resident.getDouble(offset) : readSlow(offset, Double.BYTES).getDouble();
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    PinotDataBuffer resident = residentView();
    if (resident != null) {
      resident.copyTo(offset, buffer, destOffset, size);
      return;
    }
    ByteBuffer bytes = readSlow(offset, size);
    bytes.get(buffer, destOffset, size);
  }

  @Override
  public void putByte(long offset, byte value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putChar(long offset, char value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putShort(long offset, short value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putInt(long offset, int value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putLong(long offset, long value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putFloat(long offset, float value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public void putDouble(long offset, double value) {
    throw new UnsupportedOperationException("Remote segment buffers are read-only");
  }

  @Override
  public long size() {
    return _size;
  }

  @Override
  public ByteOrder order() {
    return _order;
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    // Lazy: no fetch — the sub-view shares the same tiered read strategy
    return new RemoteSegmentBuffer(_directory, _key, _baseOffset + start, end - start, byteOrder, _dictionary);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    // Contiguous memory is required here. Promote only entries small enough to hold; for larger ones
    // fetch just the requested window so a single call cannot materialize a multi-GB entry.
    if (_size <= _directory.getMaxPromoteBytes()) {
      return promoteView().toDirectByteBuffer(offset, size, byteOrder);
    }
    byte[] bytes = new byte[size];
    _rangeFetches.incrementAndGet();
    _directory.readDataRange(_key, _baseOffset + offset, bytes, size);
    return ByteBuffer.wrap(bytes).order(byteOrder);
  }

  @Override
  public void flush() {
  }

  @Override
  public void release() {
    _cachedResident = null;
    _cachedResidentView = null;
    synchronized (_subRanges) {
      _subRanges.clear();
      _cachedSubRangeBytes = 0;
      // In-flight hints complete into an empty cache; nothing waits on them any more
      _pending.clear();
    }
  }
}
