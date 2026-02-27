package padme.store;

public final class HeapEntry {
  public final long key;
  public final double utility;

  public HeapEntry(long key, double utility) {
    this.key = key;
    this.utility = utility;
  }
}

