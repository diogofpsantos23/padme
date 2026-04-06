package padme.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public final class HeapItemStore implements ItemStore {
  private final int capacity;
  private final Map<Long, HeapEntry> byKey = new LinkedHashMap<>();
  private final List<HeapEntry> minHeap = new ArrayList<>();

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
    HeapEntry w = minHeap.isEmpty() ? null : minHeap.get(0);
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
    if (old != null) {
      removeFromHeap(old);
      old.heapIndex = -1;
    }

    if (e.representative) {
      e.heapIndex = -1;
    } else {
      addToHeap(e);
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
    removeFromHeap(e);
    e.representative = true;
  }

  @Override
  public void markNonRepresentative(long key) {
    HeapEntry e = byKey.get(key);
    if (e == null || !e.representative) return;
    e.representative = false;
    addToHeap(e);
  }

  @Override
  public boolean hasNonRepresentative() {
    return !minHeap.isEmpty();
  }

  @Override
  public void onEntryUpdated(HeapEntry e) {
    if (e == null || e.representative) return;
    int idx = e.heapIndex;
    if (idx < 0 || idx >= minHeap.size() || minHeap.get(idx) != e) return;
    fixHeapAt(idx);
  }

  @Override
  public HeapEntry evictWorst() {
    return evictWorstNonRepresentative();
  }

  @Override
  public HeapEntry evictWorstNonRepresentative() {
    HeapEntry out = pollMin();
    if (out != null) {
      byKey.remove(out.key);
    }
    return out;
  }

  @Override
  public HeapEntry evictWorstNonRepresentativeExcept(long excludedKey) {
    if (minHeap.isEmpty()) return null;

    List<HeapEntry> held = new ArrayList<>(1);
    HeapEntry out = null;

    while (!minHeap.isEmpty()) {
      HeapEntry e = pollMin();
      if (e.key == excludedKey) {
        held.add(e);
        continue;
      }
      out = e;
      break;
    }

    for (HeapEntry e : held) {
      addToHeap(e);
    }

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
    return () -> new Iterator<>() {
      private final Iterator<HeapEntry> it = byKey.values().iterator();
      private HeapEntry next = advance();

      private HeapEntry advance() {
        while (it.hasNext()) {
          HeapEntry e = it.next();
          if (!e.representative) return e;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public HeapEntry next() {
        if (next == null) throw new NoSuchElementException();
        HeapEntry current = next;
        next = advance();
        return current;
      }
    };
  }

  @Override
  public void rebuildHeap() {
    minHeap.clear();
    for (HeapEntry e : byKey.values()) {
      if (e.representative) {
        e.heapIndex = -1;
        continue;
      }
      e.heapIndex = minHeap.size();
      minHeap.add(e);
    }

    for (int i = parentIndex(minHeap.size() - 1); i >= 0; i--) {
      siftDown(i);
    }
  }

  private void addToHeap(HeapEntry e) {
    e.heapIndex = minHeap.size();
    minHeap.add(e);
    siftUp(e.heapIndex);
  }

  private HeapEntry pollMin() {
    if (minHeap.isEmpty()) return null;

    HeapEntry out = minHeap.get(0);
    HeapEntry last = minHeap.remove(minHeap.size() - 1);
    out.heapIndex = -1;

    if (!minHeap.isEmpty()) {
      minHeap.set(0, last);
      last.heapIndex = 0;
      siftDown(0);
    }

    return out;
  }

  private void removeFromHeap(HeapEntry e) {
    int idx = e.heapIndex;
    if (idx < 0 || idx >= minHeap.size() || minHeap.get(idx) != e) {
      e.heapIndex = -1;
      return;
    }

    int lastIdx = minHeap.size() - 1;
    HeapEntry last = minHeap.remove(lastIdx);
    e.heapIndex = -1;

    if (idx == lastIdx) {
      return;
    }

    minHeap.set(idx, last);
    last.heapIndex = idx;
    fixHeapAt(idx);
  }

  private void fixHeapAt(int idx) {
    if (idx <= 0) {
      siftDown(idx);
      return;
    }

    int parent = parentIndex(idx);
    if (compare(minHeap.get(idx), minHeap.get(parent)) < 0) {
      siftUp(idx);
    } else {
      siftDown(idx);
    }
  }

  private void siftUp(int idx) {
    while (idx > 0) {
      int parent = parentIndex(idx);
      if (compare(minHeap.get(idx), minHeap.get(parent)) >= 0) {
        break;
      }
      swap(idx, parent);
      idx = parent;
    }
  }

  private void siftDown(int idx) {
    int size = minHeap.size();
    while (true) {
      int left = (idx * 2) + 1;
      int right = left + 1;
      int smallest = idx;

      if (left < size && compare(minHeap.get(left), minHeap.get(smallest)) < 0) {
        smallest = left;
      }
      if (right < size && compare(minHeap.get(right), minHeap.get(smallest)) < 0) {
        smallest = right;
      }

      if (smallest == idx) {
        return;
      }

      swap(idx, smallest);
      idx = smallest;
    }
  }

  private static int compare(HeapEntry a, HeapEntry b) {
    return Double.compare(a.utility, b.utility);
  }

  private static int parentIndex(int idx) {
    return (idx - 1) / 2;
  }

  private void swap(int i, int j) {
    HeapEntry a = minHeap.get(i);
    HeapEntry b = minHeap.get(j);
    minHeap.set(i, b);
    minHeap.set(j, a);
    a.heapIndex = j;
    b.heapIndex = i;
  }
}