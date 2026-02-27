package padme.feature;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class NumericVectorMapper implements VectorMapper {
  private final boolean[] ignore;

  public NumericVectorMapper(boolean[] ignoreMask) {
    this.ignore = (ignoreMask == null) ? null : Arrays.copyOf(ignoreMask, ignoreMask.length);
  }

  public static NumericVectorMapper fromHeader(String[] header, int idColumn, String[] ignoreColumns) {
    if (header == null || header.length == 0) return new NumericVectorMapper(null);

    boolean[] mask = new boolean[header.length];

    if (idColumn >= 0 && idColumn < header.length) mask[idColumn] = true;

    Set<String> ig = new HashSet<>();
    if (ignoreColumns != null) {
      for (String s : ignoreColumns) {
        if (s != null) ig.add(s.trim());
      }
    }

    for (int i = 0; i < header.length; i++) {
      String h = header[i];
      if (h != null && ig.contains(h.trim())) mask[i] = true;
    }

    return new NumericVectorMapper(mask);
  }

  @Override
  public float[] map(String[] row, int idColumn) {
    int n = row.length;

    int outLen = 0;
    for (int i = 0; i < n; i++) {
      if (isIgnored(i, idColumn)) continue;
      outLen++;
    }

    float[] v = new float[outLen];

    int k = 0;
    for (int i = 0; i < n; i++) {
      if (isIgnored(i, idColumn)) continue;

      String raw = row[i];
      String s = (raw == null) ? "" : raw.trim();
      if (s.isEmpty()) {
        v[k++] = 0.0f;
        continue;
      }

      try {
        float f = Float.parseFloat(s);
        if (!Float.isFinite(f)) f = 0.0f;

        double x = f;
        double y = Math.signum(x) * Math.log1p(Math.abs(x));
        v[k++] = Double.isFinite(y) ? (float) y : 0.0f;

      } catch (Exception e) {
        v[k++] = 0.0f;
      }
    }

    l2NormalizeInPlace(v);
    return v;
  }

  private static void l2NormalizeInPlace(float[] v) {
    double sumSq = 0.0;
    for (float x : v) {
      if (!Float.isFinite(x)) return;
      sumSq += (double) x * (double) x;
    }

    if (!(sumSq > 0.0) || !Double.isFinite(sumSq)) return;

    double inv = 1.0 / Math.sqrt(sumSq);
    if (!Double.isFinite(inv)) return;

    for (int i = 0; i < v.length; i++) {
      double y = (double) v[i] * inv;
      v[i] = Double.isFinite(y) ? (float) y : 0.0f;
    }
  }

  private boolean isIgnored(int colIdx, int idColumn) {
    if (colIdx == idColumn) return true;
    if (ignore == null) return false;
    if (colIdx < 0 || colIdx >= ignore.length) return false;
    return ignore[colIdx];
  }
}