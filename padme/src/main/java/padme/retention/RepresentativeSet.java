package padme.retention;

import padme.math.Distance;

import java.util.ArrayList;
import java.util.List;

public final class RepresentativeSet {
  private static final class Rep {
    final float[] v;
    double utility;

    Rep(float[] v, double utility) {
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

  public int size() { return reps.size(); }

  public double utility(float[] x) {
    if (reps.isEmpty()) return Double.POSITIVE_INFINITY;
    double best = Double.POSITIVE_INFINITY;
    for (Rep r : reps) {
      double d = distance.between(x, r.v);
      if (Double.isFinite(d) && d < best) best = d;
    }
    return best;
  }

  public boolean maybeUpdate(float[] x, double uOfX) {
    if (maxRepresentatives <= 0) return false;

    float[] copy = x.clone();

    int n = reps.size();
    if (n == 0) {
      reps.add(new Rep(copy, Double.POSITIVE_INFINITY));
      return true;
    }

    double newU = Double.isFinite(uOfX) ? uOfX : 0.0;

    if (n < maxRepresentatives) {
      reps.add(new Rep(copy, newU));
      return true;
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
      reps.set(worstIdx, new Rep(copy, newU));
      return true;
    }

    return false;
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
      for (int j = 0; j < n; j++) {
        if (i == j) continue;
        double d = distance.between(ri.v, reps.get(j).v);
        if (Double.isFinite(d) && d < best) best = d;
      }

      ri.utility = Double.isFinite(best) ? best : Double.POSITIVE_INFINITY;
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

  public boolean isEmpty() {
    return reps.isEmpty();
  }
}
