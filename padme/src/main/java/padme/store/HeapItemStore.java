package padme.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public final class HeapItemStore implements ItemStore {
  private final int capacity;
  private final Map<Long, HeapEntry> byKey = new LinkedHashMap<>();
  private final PriorityQueue<HeapEntry> minHeap = new PriorityQueue<>(Comparator.comparingDouble(e -> e.utility));

  public HeapItemStore(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public int size() {
    return byKey.size();
  }

  @Override
  public int capacity() {
    return capacity;
  }

  @Override
  public double minUtility() {
    return minNonRepresentativeUtility();
  }

  @Override
  public double minNonRepresentativeUtility() {
    HeapEntry w = minHeap.peek();
    return (w == null) ? 0.0 : w.utility;
  }

  @Override
  public void add(HeapEntry e) {
    if (e.representative) addRepresentative(e);
    else addNonRepresentative(e);
  }

  @Override
  public void addRepresentative(HeapEntry e) {
    e.representative = true;
    putInternal(e);
  }

  @Override
  public void addNonRepresentative(HeapEntry e) {
    e.representative = false;
    putInternal(e);
  }

  private void putInternal(HeapEntry e) {
    HeapEntry old = byKey.put(e.key, e);
    if (old != null && !old.representative) {
      minHeap.remove(old);
    }
    if (!e.representative) {
      minHeap.add(e);
    }
  }

  @Override
  public HeapEntry get(long key) {
    return byKey.get(key);
  }

  @Override
  public boolean contains(long key) {
    return byKey.containsKey(key);
  }

  @Override
  public boolean isRepresentative(long key) {
    HeapEntry e = byKey.get(key);
    return e != null && e.representative;
  }

  @Override
  public void markRepresentative(long key) {
    HeapEntry e = byKey.get(key);
    if (e == null || e.representative) return;
    minHeap.remove(e);
    e.representative = true;
  }

  @Override
  public void markNonRepresentative(long key) {
    HeapEntry e = byKey.get(key);
    if (e == null || !e.representative) return;
    e.representative = false;
    minHeap.add(e);
  }

  @Override
  public HeapEntry evictWorst() {
    return evictWorstNonRepresentative();
  }

  @Override
  public HeapEntry evictWorstNonRepresentative() {
    HeapEntry out = minHeap.poll();
    if (out != null) {
      byKey.remove(out.key);
    }
    return out;
  }

  @Override
  public HeapEntry evictWorstNonRepresentativeExcept(long excludedKey) {
    if (minHeap.isEmpty()) return null;

    List<HeapEntry> held = new ArrayList<>();
    HeapEntry out = null;

    while (!minHeap.isEmpty()) {
      HeapEntry e = minHeap.poll();
      if (e.key == excludedKey) {
        held.add(e);
        continue;
      }
      out = e;
      break;
    }

    minHeap.addAll(held);

    if (out != null) {
      byKey.remove(out.key);
    }
    return out;
  }

  @Override
  public Iterable<HeapEntry> entries() {
    return byKey.values();
  }

  @Override
  public Iterable<HeapEntry> nonRepresentativeEntries() {
    List<HeapEntry> out = new ArrayList<>();
    for (HeapEntry e : byKey.values()) {
      if (!e.representative) out.add(e);
    }
    return out;
  }

  @Override
  public void rebuildHeap() {
    Collection<HeapEntry> values = byKey.values();
    minHeap.clear();
    for (HeapEntry e : values) {
      if (!e.representative) {
        minHeap.add(e);
      }
    }
  }
}