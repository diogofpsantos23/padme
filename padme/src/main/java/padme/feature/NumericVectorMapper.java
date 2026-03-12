package padme.feature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class NumericVectorMapper implements VectorMapper {
  public enum TransformMode {
    ZSCORE,
    LOG_ZSCORE,
    ROBUST,
    LOG_ROBUST;

    public static TransformMode fromString(String s) {
      String v = (s == null) ? "" : s.trim().toLowerCase();
      return switch (v) {
        case "zscore" -> ZSCORE;
        case "log_zscore" -> LOG_ZSCORE;
        case "robust" -> ROBUST;
        case "log_robust" -> LOG_ROBUST;
        default -> throw new IllegalArgumentException("Unknown vectorTransform: " + s);
      };
    }
  }

  private final boolean[] ignore;
  private final TransformMode mode;
  private final double[] means;
  private final double[] stds;
  private final double[] medians;
  private final double[] iqrs;

  private NumericVectorMapper(boolean[] ignoreMask, TransformMode mode, double[] means, double[] stds, double[] medians, double[] iqrs) {
    this.ignore = (ignoreMask == null) ? null : Arrays.copyOf(ignoreMask, ignoreMask.length);
    this.mode = mode;
    this.means = means;
    this.stds = stds;
    this.medians = medians;
    this.iqrs = iqrs;
  }

  public static NumericVectorMapper fit(
          String[] header,
          int idColumn,
          String[] ignoreColumns,
          List<String[]> rows,
          String vectorTransform
  ) {
    TransformMode mode = TransformMode.fromString(vectorTransform);

    if (header == null || header.length == 0) {
      return new NumericVectorMapper(null, mode, new double[0], new double[0], new double[0], new double[0]);
    }

    boolean[] mask = buildIgnoreMask(header, idColumn, ignoreColumns);

    List<Integer> keptCols = new ArrayList<>();
    for (int i = 0; i < header.length; i++) {
      if (!isIgnored(mask, i, idColumn)) keptCols.add(i);
    }

    int p = keptCols.size();
    double[] means = new double[p];
    double[] stds = new double[p];
    double[] medians = new double[p];
    double[] iqrs = new double[p];

    List<List<Double>> perCol = new ArrayList<>(p);
    for (int j = 0; j < p; j++) perCol.add(new ArrayList<>());

    if (rows != null) {
      for (String[] row : rows) {
        if (row == null) continue;

        for (int j = 0; j < p; j++) {
          int col = keptCols.get(j);
          double x = parseValue(row, col);
          if (usesLog(mode)) x = signedLog1p(x);
          perCol.get(j).add(x);
        }
      }
    }

    for (int j = 0; j < p; j++) {
      List<Double> values = perCol.get(j);
      if (values.isEmpty()) {
        means[j] = 0.0;
        stds[j] = 1.0;
        medians[j] = 0.0;
        iqrs[j] = 1.0;
        continue;
      }

      double[] a = new double[values.size()];
      double sum = 0.0;
      for (int i = 0; i < values.size(); i++) {
        a[i] = values.get(i);
        sum += a[i];
      }

      means[j] = sum / a.length;

      double var = 0.0;
      for (double v : a) {
        double d = v - means[j];
        var += d * d;
      }
      stds[j] = (a.length > 1) ? Math.sqrt(var / a.length) : 1.0;
      if (!(stds[j] > 0.0) || !Double.isFinite(stds[j])) stds[j] = 1.0;

      Arrays.sort(a);
      medians[j] = percentileSorted(a, 50.0);
      double q1 = percentileSorted(a, 25.0);
      double q3 = percentileSorted(a, 75.0);
      iqrs[j] = q3 - q1;
      if (!(iqrs[j] > 0.0) || !Double.isFinite(iqrs[j])) iqrs[j] = 1.0;
    }

    return new NumericVectorMapper(mask, mode, means, stds, medians, iqrs);
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

      double x = parseValue(row, i);
      if (usesLog(mode)) x = signedLog1p(x);

      double y;
      if (mode == TransformMode.ZSCORE || mode == TransformMode.LOG_ZSCORE) {
        y = (x - means[k]) / stds[k];
      } else {
        y = (x - medians[k]) / iqrs[k];
      }

      v[k++] = Double.isFinite(y) ? (float) y : 0.0f;
    }

    return v;
  }

  private static boolean[] buildIgnoreMask(String[] header, int idColumn, String[] ignoreColumns) {
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

    return mask;
  }

  private static boolean usesLog(TransformMode mode) {
    return mode == TransformMode.LOG_ZSCORE || mode == TransformMode.LOG_ROBUST;
  }

  private static double signedLog1p(double x) {
    double y = Math.signum(x) * Math.log1p(Math.abs(x));
    return Double.isFinite(y) ? y : 0.0;
  }

  private static double parseValue(String[] row, int col) {
    if (row == null || col < 0 || col >= row.length) return 0.0;
    String raw = row[col];
    String s = (raw == null) ? "" : raw.trim();
    if (s.isEmpty()) return 0.0;

    try {
      double x = Double.parseDouble(s);
      return Double.isFinite(x) ? x : 0.0;
    } catch (Exception e) {
      return 0.0;
    }
  }

  private static double percentileSorted(double[] a, double p) {
    if (a.length == 0) return 0.0;
    if (a.length == 1) return a[0];

    double pos = (p / 100.0) * (a.length - 1);
    int lo = (int) Math.floor(pos);
    int hi = (int) Math.ceil(pos);

    if (lo == hi) return a[lo];

    double w = pos - lo;
    return a[lo] * (1.0 - w) + a[hi] * w;
  }

  private boolean isIgnored(int colIdx, int idColumn) {
    if (colIdx == idColumn) return true;
    if (ignore == null) return false;
    if (colIdx < 0 || colIdx >= ignore.length) return false;
    return ignore[colIdx];
  }

  private static boolean isIgnored(boolean[] ignore, int colIdx, int idColumn) {
    if (colIdx == idColumn) return true;
    if (ignore == null) return false;
    if (colIdx < 0 || colIdx >= ignore.length) return false;
    return ignore[colIdx];
  }
}