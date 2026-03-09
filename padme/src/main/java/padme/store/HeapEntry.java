package padme.store;

public final class HeapEntry {
  public final long key;
  public final float[] vector;
  public double utility;
  public long nearestRepKey;
  public boolean representative;

  public HeapEntry(long key, double utility) {
    this(key, null, utility, -1L, false);
  }

  public HeapEntry(long key, float[] vector, double utility, long nearestRepKey) {
    this(key, vector, utility, nearestRepKey, false);
  }

  public HeapEntry(long key, float[] vector, double utility, long nearestRepKey, boolean representative) {
    this.key = key;
    this.vector = vector;
    this.utility = utility;
    this.nearestRepKey = nearestRepKey;
    this.representative = representative;
  }
}