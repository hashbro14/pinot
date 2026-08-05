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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

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
  /** Cap for the load-mode sub-range cache; past it, reads promote the whole entry instead. */
  private static final long MAX_CACHED_SUB_RANGE_BYTES = 1L << 20;
  /** Small reads are rounded up to this granularity to spare repeated header fetches. */
  private static final int MIN_FETCH_BYTES = 4096;

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

  RemoteSegmentBuffer(RemoteSegmentDirectory directory, IndexKey key, long baseOffset, long size, ByteOrder order) {
    super(false);
    _directory = directory;
    _key = key;
    _baseOffset = baseOffset;
    _size = size;
    _order = order;
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

  private PinotDataBuffer promoteView() {
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
    // 1. Cached sub-range?
    synchronized (_subRanges) {
      Map.Entry<Long, byte[]> floor = _subRanges.floorEntry(offset);
      if (floor != null && floor.getKey() + floor.getValue().length >= offset + length) {
        return ByteBuffer.wrap(floor.getValue(), Math.toIntExact(offset - floor.getKey()), length).order(_order);
      }
    }
    // 2. Query mode or oversized: promote the whole entry (no monitor held)
    if (_directory.inQueryMode() || length > MAX_CACHED_SUB_RANGE_BYTES) {
      PinotDataBuffer view = promoteView();
      byte[] bytes = new byte[length];
      view.copyTo(offset, bytes, 0, length);
      return ByteBuffer.wrap(bytes).order(_order);
    }
    // 3. Load mode: exact small ranged fetch (no monitor held), then cache under the monitor
    int fetchLength = Math.toIntExact(Math.min(Math.max(length, MIN_FETCH_BYTES), _size - offset));
    byte[] fetched = new byte[fetchLength];
    _directory.readDataRange(_key, _baseOffset + offset, fetched, fetchLength);
    synchronized (_subRanges) {
      if (_cachedSubRangeBytes + fetchLength <= MAX_CACHED_SUB_RANGE_BYTES && !_subRanges.containsKey(offset)) {
        _subRanges.put(offset, fetched);
        _cachedSubRangeBytes += fetchLength;
      }
    }
    return ByteBuffer.wrap(fetched, 0, length).order(_order);
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
    return new RemoteSegmentBuffer(_directory, _key, _baseOffset + start, end - start, byteOrder);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    // Contiguous memory is required here, so this is the one read path that forces residency
    return promoteView().toDirectByteBuffer(offset, size, byteOrder);
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
    }
  }
}
