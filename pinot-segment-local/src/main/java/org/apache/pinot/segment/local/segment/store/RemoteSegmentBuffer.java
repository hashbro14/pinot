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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lazy {@link PinotDataBuffer} over (a range of) one index entry of a remote segment's {@code columns.psf}.
 *
 * <p>Reads follow a three-tier strategy:
 * <ol>
 *   <li><b>Resident fast path</b> — when the whole entry is resident in the directory (query prefetch or an
 *       earlier promotion), reads delegate to the in-memory buffer. Residency is re-checked by identity on
 *       every read, so an entry freed by the directory is not retained here and its memory can be reclaimed;
 *       a later read simply falls to the tiers below (or re-promotes).</li>
 *   <li><b>Load-mode small reads</b> — with no query active (index reader construction at segment load),
 *       reads are served as exact small ranged fetches, kept in a bounded per-buffer cache so repeated
 *       header reads do not refetch. This keeps segment load from downloading index data wholesale.</li>
 *   <li><b>Query-mode miss</b> — a read while queries are active but the entry is not resident (planner
 *       under-fetch) promotes the whole entry synchronously, then serves from it.</li>
 * </ol>
 *
 * <p>{@link #view} returns another lazy buffer over the sub-range (no fetch); only
 * {@link #toDirectByteBuffer} forces the entry resident, because it must hand out contiguous memory.
 * The buffer is read-only — all mutating methods throw.
 *
 * <p>Thread-safety: reads may come from multiple query threads. The sub-range cache is guarded by its own
 * monitor and never holds the monitor across network fetches; the resident view cache uses benign races
 * (worst case a rebuilt view).
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
  private static final int VAR_LENGTH_HEADER_BYTES = 16;
  /** Refuse to prime absurdly large offset tables; those fall back to ordinary ranged reads. */
  private static final long MAX_OFFSET_TABLE_BYTES = 4L << 20;
  /** Two prefetched ranges closer than this are merged: one bigger GET beats two with a hole between them. */
  private static final long PREFETCH_COALESCE_GAP_BYTES = RemoteQueryConfigs.prefetchCoalesceGapBytes();
  /** Give up on prefetching rather than blow the sub-range cache. */
  private static final long MAX_PREFETCH_BYTES = RemoteQueryConfigs.prefetchMaxBytes();
  /** Prefetching is declined once a batch spans more than 1/this of the entry. */
  private static final int WHOLE_ENTRY_PREFETCH_RATIO = 4;
  /** Hard ceiling on requests issued for one batch; past it, reading the entry whole is cheaper. */
  private static final int MAX_PREFETCH_RANGES = RemoteQueryConfigs.prefetchMaxRanges();

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
  /** End of the previous ranged read, used to tell a scan apart from random lookups. */
  private volatile long _lastReadEnd = -1;
  /** True when this entry is a dictionary, whose offset table is worth fetching up front. */
  private final boolean _dictionary;
  private volatile boolean _offsetTablePrimed;
  private volatile byte[] _offsetTable;
  private volatile int _offsetTableStart;
  /** Ranged reads this buffer has actually issued; used by tests and for diagnosing fetch patterns. */
  private final AtomicLong _rangeFetches = new AtomicLong();
  /** Random (non-sequential) reads served so far; drives escalation to a single bulk fetch. */
  private final java.util.concurrent.atomic.AtomicInteger _randomReads =
      new java.util.concurrent.atomic.AtomicInteger();

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
   * Reads the offset table of a variable-length dictionary in one shot and keeps it for this buffer's life.
   *
   * <p>Resolving one value of such a dictionary is three dependent reads: two into the offset table to learn
   * where the value lives, then the value itself. Locally those are page-cache hits; against object storage
   * each is a round trip, so a thousand-row projection becomes thousands of requests. The offset table is
   * contiguous ((numValues + 1) * 4 bytes), so fetching it once turns every later lookup into a single read
   * for the value bytes alone.
   *
   * <p>No-op for fixed-width dictionaries, whose value positions are computed arithmetically.
   */
  private void primeDictionaryOffsets() {
    if (_offsetTable != null || _offsetTablePrimed) {
      return;
    }
    _offsetTablePrimed = true;
    try {
      byte[] header = new byte[VAR_LENGTH_HEADER_BYTES];
      _rangeFetches.incrementAndGet();
      _directory.readDataRange(_key, _baseOffset, header, header.length);
      ByteBuffer head = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < VAR_LENGTH_MAGIC.length; i++) {
        if (header[i] != VAR_LENGTH_MAGIC[i]) {
          return; // fixed-width dictionary: nothing to prime
        }
      }
      int numValues = head.getInt(VAR_LENGTH_NUM_VALUES_OFFSET);
      int offsetTableStart = head.getInt(VAR_LENGTH_DATA_SECTION_OFFSET_POSITION);
      long offsetTableBytes = (numValues + 1L) * Integer.BYTES;
      if (numValues <= 0 || offsetTableStart < 0 || offsetTableStart + offsetTableBytes > _size
          || offsetTableBytes > MAX_OFFSET_TABLE_BYTES) {
        return;
      }
      byte[] table = new byte[Math.toIntExact(offsetTableBytes)];
      _rangeFetches.incrementAndGet();
      _directory.readDataRange(_key, _baseOffset + offsetTableStart, table, table.length);
      _offsetTableStart = offsetTableStart;
      _offsetTable = table;
    } catch (RuntimeException e) {
      // Priming is an optimization; fall back to ordinary ranged reads
      _offsetTable = null;
    }
  }

  /** Serves a read out of the primed offset table when it covers the requested range. */
  private ByteBuffer readFromOffsetTable(long offset, int length) {
    byte[] table = _offsetTable;
    if (table == null || offset < _offsetTableStart || offset + length > _offsetTableStart + table.length) {
      return null;
    }
    return ByteBuffer.wrap(table, Math.toIntExact(offset - _offsetTableStart), length).order(_order);
  }

  /**
   * Returns a view of the resident entry matching this buffer's range and order, or null when the entry is
   * not resident. Re-checked by identity per call so directory frees are honored.
   */
  private PinotDataBuffer residentView() {
    PinotDataBuffer resident = _directory.peekResident(_key);
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
    // 0. Dictionary offset table: primed once, then served from memory
    if (_dictionary) {
      primeDictionaryOffsets();
      ByteBuffer fromTable = readFromOffsetTable(offset, length);
      if (fromTable != null) {
        return fromTable;
      }
    }
    // 1. Cached sub-range?
    synchronized (_subRanges) {
      Map.Entry<Long, byte[]> floor = _subRanges.floorEntry(offset);
      if (floor != null && floor.getKey() + floor.getValue().length >= offset + length) {
        return ByteBuffer.wrap(floor.getValue(), Math.toIntExact(offset - floor.getKey()), length).order(_order);
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
    if (_cachedResident != null) {
      // Already materialized locally: reads are memory-speed and hinting only adds work
      return false;
    }
    return true;
  }

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
      // Reading the entry whole is now the cheaper plan (a full scan looks like this, and so does a
      // batch too scattered to merge). Decline; the promote-on-miss path pulls it in one bounded read.
      return;
    }
    List<long[]> toFetch = new ArrayList<>(merged.size());
    List<byte[]> targets = new ArrayList<>(merged.size());
    for (long[] range : merged) {
      long start = range[0];
      int len = Math.toIntExact(Math.min(range[1] - start, _size - start));
      if (len <= 0) {
        continue;
      }
      synchronized (_subRanges) {
        Map.Entry<Long, byte[]> floor = _subRanges.floorEntry(start);
        if (floor != null && floor.getKey() + floor.getValue().length >= start + len) {
          continue;
        }
      }
      // element 0 is the offset to read, element 1 the key this range is cached under
      toFetch.add(new long[]{_baseOffset + start, start});
      targets.add(new byte[len]);
    }
    if (toFetch.isEmpty()) {
      return;
    }
    // One call, so the batch's ranges are all in flight together rather than each waiting its turn
    _rangeFetches.addAndGet(toFetch.size());
    _directory.readDataRanges(_key, toFetch, targets);
    for (int i = 0; i < toFetch.size(); i++) {
      cacheSubRange(toFetch.get(i)[1], targets.get(i));
    }
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
    _offsetTable = null;
    _offsetTablePrimed = false;
    synchronized (_subRanges) {
      _subRanges.clear();
      _cachedSubRangeBytes = 0;
    }
  }
}
