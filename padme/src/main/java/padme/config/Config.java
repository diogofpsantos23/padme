package padme.config;

import java.util.List;

public final class Config {
  public String path;
  public String separator = ",";
  public int idColumn = -1;

  public int refreshUtilitySpan = 20;
  public String mode = "baseline";

  public int nodes = 1;

  public Integer pssViewSize;
  public Integer pssShuffleLength;
  public int pssCycleEveryItems = 50;

  public Integer replFanout;
  public int replBatchSize = 64;
  public int replCycleEveryItems = 50;
  public Integer replTtl;
  public int replSeenCacheSize = 100_000;

  public double padmeBinBalanceGamma = 0.3;
  public double padmeBinBalanceMin = 0.8;
  public double padmeBinBalanceMax = 1.2;

  public double keepRatio = 0.1;
  public List<Double> dataKeepRatios;

  public Integer maxStoredItems;
  public Integer maxRepresentatives;
  public int nonRepSampleFactor = 10;

  public int reportEvery = 1000;

  public String[] ignoreColumns;

  public String vectorTransform;

  public void validate() {
    if (path == null || path.isBlank())
      throw new IllegalArgumentException("config.path is required");

    if (separator == null || separator.isEmpty())
      throw new IllegalArgumentException("config.separator is required");

    if (mode == null || mode.isBlank())
      throw new IllegalArgumentException("config.mode is required");

    if (nodes <= 0)
      throw new IllegalArgumentException("config.nodes must be > 0");

    if (pssViewSize != null && pssViewSize <= 0)
      throw new IllegalArgumentException("config.pssViewSize must be > 0 when provided");

    if (pssShuffleLength != null && pssShuffleLength <= 0)
      throw new IllegalArgumentException("config.pssShuffleLength must be > 0 when provided");

    if (pssCycleEveryItems <= 0)
      throw new IllegalArgumentException("config.pssCycleEveryItems must be > 0");

    if (replFanout != null && replFanout <= 0)
      throw new IllegalArgumentException("config.replFanout must be > 0 when provided");

    if (replBatchSize <= 0)
      throw new IllegalArgumentException("config.replBatchSize must be > 0");

    if (replCycleEveryItems <= 0)
      throw new IllegalArgumentException("config.replCycleEveryItems must be > 0");

    if (replTtl != null && replTtl <= 0)
      throw new IllegalArgumentException("config.replTtl must be > 0 when provided");

    if (replSeenCacheSize <= 0)
      throw new IllegalArgumentException("config.replSeenCacheSize must be > 0");

    if (padmeBinBalanceGamma < 0.0)
      throw new IllegalArgumentException("config.padmeBinBalanceGamma must be >= 0");

    if (padmeBinBalanceMin <= 0.0)
      throw new IllegalArgumentException("config.padmeBinBalanceMin must be > 0");

    if (padmeBinBalanceMax <= 0.0)
      throw new IllegalArgumentException("config.padmeBinBalanceMax must be > 0");

    if (padmeBinBalanceMin > padmeBinBalanceMax)
      throw new IllegalArgumentException("config.padmeBinBalanceMin must be <= config.padmeBinBalanceMax");

    if (keepRatio <= 0.0 || keepRatio > 1.0)
      throw new IllegalArgumentException("config.keepRatio must be in (0,1]");

    if (maxStoredItems != null && maxStoredItems <= 0)
      throw new IllegalArgumentException("config.maxStoredItems must be > 0");

    if (reportEvery <= 0)
      throw new IllegalArgumentException("config.reportEvery must be > 0");

    if (vectorTransform == null || vectorTransform.isBlank()) {
      vectorTransform = "log_zscore";
    }

    String vt = vectorTransform.trim().toLowerCase();
    if (!vt.equals("zscore") &&
            !vt.equals("log_zscore") &&
            !vt.equals("robust") &&
            !vt.equals("log_robust")) {
      throw new IllegalArgumentException("config.vectorTransform must be one of: zscore, log_zscore, robust, log_robust");
    }
    vectorTransform = vt;

    String m = mode.trim().toLowerCase();
    m = switch (m) {
      case "graphcut", "graph_cut" -> "graph_cut";
      case "maxdiversity", "max_diversity" -> "max_diversity";
      case "kcenter", "k_center" -> "k_center";
      case "baseline", "random", "padme" -> m;
      default -> m;
    };

    if (!m.equals("baseline") &&
            !m.equals("random") &&
            !m.equals("padme") &&
            !m.equals("graph_cut") &&
            !m.equals("max_diversity") &&
            !m.equals("k_center")) {
      throw new IllegalArgumentException("config.mode must be one of: baseline, random, padme, graph_cut, max_diversity, k_center");
    }
    mode = m;

    if (mode.equals("padme") ||
            mode.equals("graph_cut") ||
            mode.equals("max_diversity") ||
            mode.equals("k_center")) {

      if (maxStoredItems != null && maxStoredItems <= 0)
        throw new IllegalArgumentException("config.maxStoredItems must be > 0 when mode=" + mode);

      if (nonRepSampleFactor < 1 && !mode.equals("k_center"))
        throw new IllegalArgumentException("config.nonRepSampleFactor must be > 1 when mode=" + mode);

      if (maxRepresentatives != null && maxRepresentatives <= 0)
        throw new IllegalArgumentException("config.maxRepresentatives must be > 0 when provided");

      if (maxStoredItems != null && maxRepresentatives != null && maxRepresentatives >= maxStoredItems)
        throw new IllegalArgumentException("config.maxRepresentatives must be < config.maxStoredItems when provided");

      if (refreshUtilitySpan < 0)
        throw new IllegalArgumentException("config.refreshUtilitySpan must be >= 0 when mode=" + mode);
    }
  }

  @Override
  public String toString() {
    return "Config{" +
            "path='" + path + '\'' +
            ", separator='" + separator + '\'' +
            ", idColumn=" + idColumn +
            ", refreshUtilities=" + refreshUtilitySpan +
            ", mode='" + mode + '\'' +
            ", nodes=" + nodes +
            ", pssViewSize=" + pssViewSize +
            ", pssShuffleLength=" + pssShuffleLength +
            ", pssCycleEveryItems=" + pssCycleEveryItems +
            ", replFanout=" + replFanout +
            ", replBatchSize=" + replBatchSize +
            ", replCycleEveryItems=" + replCycleEveryItems +
            ", replTtl=" + replTtl +
            ", replSeenCacheSize=" + replSeenCacheSize +
            ", padmeBinBalanceGamma=" + padmeBinBalanceGamma +
            ", padmeBinBalanceMin=" + padmeBinBalanceMin +
            ", padmeBinBalanceMax=" + padmeBinBalanceMax +
            ", keepRatio=" + keepRatio +
            ", maxStoredItems=" + maxStoredItems +
            ", maxRepresentatives=" + maxRepresentatives +
            ", reportEvery=" + reportEvery +
            ", ignoreColumns=" + java.util.Arrays.toString(ignoreColumns) +
            ", vectorTransform='" + vectorTransform + '\'' +
            '}';
  }
}
