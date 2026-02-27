package padme.store;

public interface ItemStore {
  int size();
  int capacity();
  double minUtility();
  void add(HeapEntry e);
  HeapEntry evictWorst();
}
