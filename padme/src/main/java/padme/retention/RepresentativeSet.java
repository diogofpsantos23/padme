package padme.retention;

import padme.math.Distance;

import java.util.ArrayList;
import java.util.List;

public final class RepresentativeSet {
  public record UtilityScore(long nearestRepKey, double utility) {
  }

  public record NeighborScore(long nearestRepKey, double nearestUtility, long secondNearestRepKey, double secondNearestUtility) {
  }

  public static final class Change {
    public final boolean changed;
    public final boolean membershipChanged;
    public final long addedRepKey;
    public final long removedRepKey;
    public final long updatedRepKey;

    private Change(boolean changed, boolean membershipChanged, long addedRepKey, long removedRepKey, long updatedRepKey) {
      this.changed = changed;
      this.membershipChanged = membershipChanged;
      this.addedRepKey = addedRepKey;
      this.removedRepKey = removedRepKey;
      this.updatedRepKey = updatedRepKey;
    }

    public static Change none() {
      return new Change(false, false, -1L, -1L, -1L);
    }

    public static Change added(long key) {
      return new Change(true, true, key, -1L, -1L);
    }

    public static Change replaced(long addedKey, long removedKey) {
      return new Change(true, true, addedKey, removedKey, -1L);
    }

    public static Change updated(long key) {
      return new Change(true, false, -1L, -1L, key);
    }
  }

  private static final class Rep {
    final long key;
    final float[] v;
    double utility;

    Rep(long key, float[] v, double utility) {
      this.key = key;
      this.v = v;
      this.utility = utility;
    }
  }

  private final int maxRepresentatives;
  private final Distance distance;
  private final List<Rep> reps = new ArrayList<>();

  public RepresentativeSet(int maxRepresentatives, Distance distance) {
    this.maxRepresentatives = Math.max(0, maxRepresentatives);
    this.distance = distance;
  }

  public static double computeRepresentativeUtility(double nearestDistance, double secondNearestDistance) {
    if (!Double.isFinite(nearestDistance)) {
      return Double.POSITIVE_INFINITY;
    }

    double base = Math.log1p(Math.max(0.0, nearestDistance));

    if (!Double.isFinite(secondNearestDistance) || secondNearestDistance <= 0.0) {
      return base;
    }

    double ratio = nearestDistance / secondNearestDistance;
    if (ratio < 0.0) ratio = 0.0;
    if (ratio > 1.0) ratio = 1.0;

    return base * (1.0 + 0.25 * ratio);
  }

  public int size() {
    return reps.size();
  }

  public boolean isEmpty() {
    return reps.isEmpty();
  }

  public boolean isFull() {
    return reps.size() >= maxRepresentatives;
  }

  public UtilityScore score(long itemKey, float[] x) {
    NeighborScore s = scoreWithSecond(itemKey, x);
    return new UtilityScore(s.nearestRepKey, s.nearestUtility);
  }

  public NeighborScore scoreWithSecond(long itemKey, float[] x) {
    if (reps.isEmpty()) {
      return new NeighborScore(-1L, Double.POSITIVE_INFINITY, -1L, Double.POSITIVE_INFINITY);
    }

    double best = Double.POSITIVE_INFINITY;
    long bestKey = -1L;

    double second = Double.POSITIVE_INFINITY;
    long secondKey = -1L;

    for (Rep r : reps) {
      if (r.key == itemKey) continue;

      double d = distance.between(x, r.v);
      if (!Double.isFinite(d)) continue;

      if (d < best) {
        second = best;
        secondKey = bestKey;
        best = d;
        bestKey = r.key;
      } else if (d < second) {
        second = d;
        secondKey = r.key;
      }
    }

    if (bestKey < 0L) {
      return new NeighborScore(-1L, Double.POSITIVE_INFINITY, -1L, Double.POSITIVE_INFINITY);
    }

    return new NeighborScore(bestKey, best, secondKey, second);
  }

  public UtilityScore bestExcluding(long itemKey, float[] x, long excludedRepKey) {
    double best = Double.POSITIVE_INFINITY;
    long bestKey = -1L;

    for (Rep r : reps) {
      if (r.key == itemKey || r.key == excludedRepKey) continue;

      double d = distance.between(x, r.v);
      if (Double.isFinite(d) && d < best) {
        best = d;
        bestKey = r.key;
      }
    }

    if (bestKey < 0L) {
      return new UtilityScore(-1L, Double.POSITIVE_INFINITY);
    }

    return new UtilityScore(bestKey, best);
  }

  public double utility(long itemKey, float[] x) {
    return score(itemKey, x).utility;
  }

  public double distanceToRep(long repKey, float[] x) {
    for (Rep r : reps) {
      if (r.key == repKey) {
        return distance.between(x, r.v);
      }
    }
    return Double.POSITIVE_INFINITY;
  }

  public Change maybeUpdate(long key, float[] x, double repUtilityOfX) {
    if (maxRepresentatives <= 0) return Change.none();

    float[] copy = x.clone();
    double newU = Double.isFinite(repUtilityOfX) ? repUtilityOfX : 0.0;

    int existingIdx = indexOfKey(key);
    if (existingIdx >= 0) {
      reps.set(existingIdx, new Rep(key, copy, newU));
      return Change.updated(key);
    }

    int n = reps.size();
    if (n == 0) {
      reps.add(new Rep(key, copy, Double.POSITIVE_INFINITY));
      return Change.added(key);
    }

    if (n < maxRepresentatives) {
      reps.add(new Rep(key, copy, newU));
      return Change.added(key);
    }

    int worstIdx = 0;
    double worstU = worstScore(reps.getFirst().utility);

    for (int i = 1; i < n; i++) {
      double u = worstScore(reps.get(i).utility);
      if (u < worstU) {
        worstU = u;
        worstIdx = i;
      }
    }

    if (newU > worstU) {
      long removedKey = reps.get(worstIdx).key;
      reps.set(worstIdx, new Rep(key, copy, newU));
      return Change.replaced(key, removedKey);
    }

    return Change.none();
  }

  private int indexOfKey(long key) {
    for (int i = 0; i < reps.size(); i++) {
      if (reps.get(i).key == key) return i;
    }
    return -1;
  }

  private static double worstScore(double u) {
    return Double.isFinite(u) ? u : Double.NEGATIVE_INFINITY;
  }

  public void refreshUtilities() {
    int n = reps.size();
    if (n == 0) return;

    if (n == 1) {
      reps.getFirst().utility = Double.POSITIVE_INFINITY;
      return;
    }

    for (int i = 0; i < n; i++) {
      Rep ri = reps.get(i);

      double best = Double.POSITIVE_INFINITY;
      double second = Double.POSITIVE_INFINITY;

      for (int j = 0; j < n; j++) {
        if (i == j) continue;

        double d = distance.between(ri.v, reps.get(j).v);
        if (!Double.isFinite(d)) continue;

        if (d < best) {
          second = best;
          best = d;
        } else if (d < second) {
          second = d;
        }
      }

      ri.utility = computeRepresentativeUtility(best, second);
    }
  }

  public double minUtility() {
    if (reps.isEmpty()) return 0.0;

    double min = Double.POSITIVE_INFINITY;
    for (Rep r : reps) {
      double u = r.utility;
      if (Double.isFinite(u) && u < min) min = u;
    }
    return min == Double.POSITIVE_INFINITY ? 0.0 : min;
  }

  public double meanUtility() {
    if (reps.isEmpty()) return 0.0;

    double sum = 0.0;
    int c = 0;
    for (Rep r : reps) {
      double u = r.utility;
      if (Double.isFinite(u)) {
        sum += u;
        c++;
      }
    }
    return c == 0 ? 0.0 : sum / (double) c;
  }
}