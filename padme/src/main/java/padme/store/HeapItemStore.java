package padme.store;

import java.util.Comparator;
import java.util.PriorityQueue;

public final class HeapItemStore implements ItemStore {
  private final int capacity;
  private final PriorityQueue<HeapEntry> minHeap = new PriorityQueue<>(Comparator.comparingDouble(e -> e.utility));

  public HeapItemStore(int capacity) {
    this.capacity = capacity;
  }

  @Override public int size() { return minHeap.size(); }
  @Override public int capacity() { return capacity; }

  @Override
  public double minUtility() {
    HeapEntry w = minHeap.peek();
    return (w == null) ? 0.0 : w.utility;
  }

  @Override
  public void add(HeapEntry e) {
    minHeap.add(e);
  }

  @Override
  public HeapEntry evictWorst() {
    return minHeap.poll();
  }
}
