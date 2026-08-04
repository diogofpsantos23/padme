package padme.retention;

import padme.math.Distance;

import java.util.ArrayList;
import java.util.List;

public final class RepresentativeSet {
  public static final double DEFAULT_ALPHA = 0.25;

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
    long nearestRepKey;
    double nearestDistance;
    long secondNearestRepKey;
    double secondNearestDistance;
    double utility;

    Rep(long key, float[] v) {
      this.key = key;
      this.v = v;
      this.nearestRepKey = -1L;
      this.nearestDistance = Double.POSITIVE_INFINITY;
      this.secondNearestRepKey = -1L;
      this.secondNearestDistance = Double.POSITIVE_INFINITY;
      this.utility = Double.POSITIVE_INFINITY;
    }
  }

  private final int maxRepresentatives;
  private final Distance distance;
  private final double alpha;
  private final List<Rep> reps = new ArrayList<>();
  private boolean utilitiesCurrent = true;

  public RepresentativeSet(int maxRepresentatives, Distance distance) {
    this(maxRepresentatives, distance, DEFAULT_ALPHA);
  }

  public RepresentativeSet(int maxRepresentatives, Distance distance, double alpha) {
    this.maxRepresentatives = Math.max(0, maxRepresentatives);
    this.distance = distance;
    this.alpha = Math.max(0.0, alpha);
  }

  public static double computeRepresentativeUtility(double nearestDistance, double secondNearestDistance) {
    return computeRepresentativeUtility(nearestDistance, secondNearestDistance, DEFAULT_ALPHA);
  }

  public static double computeRepresentativeUtility(double nearestDistance, double secondNearestDistance, double alpha) {
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
    return base * (1.0 + Math.max(0.0, alpha) * ratio);
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

  public double distanceBetween(float[] a, float[] b) {
    return distance.between(a, b);
  }

  public Change maybeUpdate(long key, float[] x, double repUtilityOfX) {
    if (maxRepresentatives <= 0) return Change.none();

    float[] copy = x.clone();
    double candidateUtility = Double.isFinite(repUtilityOfX) ? repUtilityOfX : 0.0;
    int existingIdx = indexOfKey(key);

    if (existingIdx >= 0) {
      replaceRepresentative(existingIdx, new Rep(key, copy));
      return Change.updated(key);
    }

    int n = reps.size();
    if (n == 0) {
      reps.add(new Rep(key, copy));
      utilitiesCurrent = true;
      return Change.added(key);
    }

    if (n < maxRepresentatives) {
      addRepresentative(new Rep(key, copy));
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

    if (candidateUtility > worstU) {
      long removedKey = reps.get(worstIdx).key;
      replaceRepresentative(worstIdx, new Rep(key, copy));
      return Change.replaced(key, removedKey);
    }

    return Change.none();
  }

  private void addRepresentative(Rep added) {
    int existingCount = reps.size();
    reps.add(added);

    for (int i = 0; i < existingCount; i++) {
      Rep existing = reps.get(i);
      double d = distance.between(existing.v, added.v);
      if (!Double.isFinite(d)) continue;

      insertNeighbor(existing, added.key, d);
      insertNeighbor(added, existing.key, d);
    }

    refreshUtility(added);
    utilitiesCurrent = true;
  }

  private void replaceRepresentative(int index, Rep replacement) {
    Rep removed = reps.get(index);
    long removedKey = removed.key;
    reps.set(index, replacement);

    int n = reps.size();
    for (int i = 0; i < n; i++) {
      Rep current = reps.get(i);
      if (current.key == replacement.key) continue;

      boolean dependedOnRemoved = current.nearestRepKey == removedKey || current.secondNearestRepKey == removedKey;
      if (dependedOnRemoved) {
        recomputeNeighbors(current);
      } else {
        double d = distance.between(current.v, replacement.v);
        if (Double.isFinite(d)) {
          insertNeighbor(current, replacement.key, d);
        }
      }
    }

    recomputeNeighbors(replacement);
    utilitiesCurrent = true;
  }

  private void insertNeighbor(Rep target, long candidateKey, double candidateDistance) {
    if (target.key == candidateKey || !Double.isFinite(candidateDistance)) return;

    if (target.nearestRepKey == candidateKey) {
      target.nearestDistance = candidateDistance;
      normalizeNeighbors(target);
      refreshUtility(target);
      return;
    }

    if (target.secondNearestRepKey == candidateKey) {
      target.secondNearestDistance = candidateDistance;
      normalizeNeighbors(target);
      refreshUtility(target);
      return;
    }

    if (!Double.isFinite(target.nearestDistance) || candidateDistance < target.nearestDistance) {
      target.secondNearestRepKey = target.nearestRepKey;
      target.secondNearestDistance = target.nearestDistance;
      target.nearestRepKey = candidateKey;
      target.nearestDistance = candidateDistance;
      refreshUtility(target);
      return;
    }

    if (!Double.isFinite(target.secondNearestDistance) || candidateDistance < target.secondNearestDistance) {
      target.secondNearestRepKey = candidateKey;
      target.secondNearestDistance = candidateDistance;
      refreshUtility(target);
    }
  }

  private void normalizeNeighbors(Rep rep) {
    if (rep.secondNearestDistance < rep.nearestDistance) {
      long key = rep.nearestRepKey;
      double d = rep.nearestDistance;
      rep.nearestRepKey = rep.secondNearestRepKey;
      rep.nearestDistance = rep.secondNearestDistance;
      rep.secondNearestRepKey = key;
      rep.secondNearestDistance = d;
    }
  }

  private void recomputeNeighbors(Rep target) {
    target.nearestRepKey = -1L;
    target.nearestDistance = Double.POSITIVE_INFINITY;
    target.secondNearestRepKey = -1L;
    target.secondNearestDistance = Double.POSITIVE_INFINITY;

    for (Rep candidate : reps) {
      if (candidate.key == target.key) continue;

      double d = distance.between(target.v, candidate.v);
      if (!Double.isFinite(d)) continue;

      if (d < target.nearestDistance) {
        target.secondNearestRepKey = target.nearestRepKey;
        target.secondNearestDistance = target.nearestDistance;
        target.nearestRepKey = candidate.key;
        target.nearestDistance = d;
      } else if (d < target.secondNearestDistance) {
        target.secondNearestRepKey = candidate.key;
        target.secondNearestDistance = d;
      }
    }

    refreshUtility(target);
  }

  private void refreshUtility(Rep rep) {
    rep.utility = computeRepresentativeUtility(rep.nearestDistance, rep.secondNearestDistance, alpha);
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
    if (!utilitiesCurrent) {
      rebuildUtilities();
    }
  }

  public void rebuildUtilities() {
    for (Rep rep : reps) {
      recomputeNeighbors(rep);
    }
    utilitiesCurrent = true;
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
