package padme.store;

public interface ItemStore {
  int size();
  int capacity();
  double minUtility();
  double minNonRepresentativeUtility();
  void add(HeapEntry e);
  void addRepresentative(HeapEntry e);
  void addNonRepresentative(HeapEntry e);
  HeapEntry get(long key);
  boolean contains(long key);
  boolean isRepresentative(long key);
  void markRepresentative(long key);
  void markNonRepresentative(long key);
  HeapEntry evictWorst();
  HeapEntry evictWorstNonRepresentative();
  HeapEntry evictWorstNonRepresentativeExcept(long excludedKey);
  Iterable<HeapEntry> entries();
  Iterable<HeapEntry> nonRepresentativeEntries();
  void rebuildHeap();
}