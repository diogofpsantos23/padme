package padme.math;

public final class L2Distance implements Distance {
  @Override
  public double between(float[] a, float[] b) {
    int n = Math.min(a.length, b.length);
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
      double ai = a[i], bi = b[i];
      if (!Double.isFinite(ai) || !Double.isFinite(bi)) return Double.NaN;
      double d = ai - bi;
      sum += d * d;
    }
    return Math.sqrt(sum);
  }
}
