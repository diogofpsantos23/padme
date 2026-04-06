package padme.store;

public final class HeapEntry {
  public final long key;
  public final float[] vector;

  public double utility;
  public double representativeUtility;

  public double nearestDistance;
  public long nearestRepKey;

  public double secondNearestDistance;
  public long secondNearestRepKey;

  public boolean representative;
  public int heapIndex = -1;

  public HeapEntry(long key, double utility) {
    this(key, null, utility, utility, Double.POSITIVE_INFINITY, -1L, Double.POSITIVE_INFINITY, -1L, false);
  }

  public HeapEntry(long key, float[] vector, double utility, long nearestRepKey) {
    this(key, vector, utility, utility, Double.POSITIVE_INFINITY, nearestRepKey, Double.POSITIVE_INFINITY, -1L, false);
  }

  public HeapEntry(long key, float[] vector, double utility, long nearestRepKey, boolean representative) {
    this(key, vector, utility, utility, Double.POSITIVE_INFINITY, nearestRepKey, Double.POSITIVE_INFINITY, -1L, representative);
  }

  public HeapEntry(long key, float[] vector, double utility, double representativeUtility, double nearestDistance, long nearestRepKey, double secondNearestDistance, long secondNearestRepKey, boolean representative) {
    this.key = key;
    this.vector = vector;
    this.utility = utility;
    this.representativeUtility = representativeUtility;
    this.nearestDistance = nearestDistance;
    this.nearestRepKey = nearestRepKey;
    this.secondNearestDistance = secondNearestDistance;
    this.secondNearestRepKey = secondNearestRepKey;
    this.representative = representative;
  }
}