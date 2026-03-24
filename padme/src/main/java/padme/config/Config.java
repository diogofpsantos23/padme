package padme.config;

import java.util.List;

public final class Config {
  public String path;
  public String separator = ",";
  public int idColumn = -1;

  public int refreshUtilitySpan = 16;
  public String mode = "baseline";

  public int nodes = 1;

  public int pssViewSize = 4;
  public int pssShuffleLength = 4;
  public int pssCycleEveryItems = 50;

  public int replFanout = 2;
  public int replBatchSize = 32;
  public int replCycleEveryItems = 50;
  public int replTtl = 2;

  public double padmeBinBalanceGamma = 1.0;
  public double padmeBinBalanceMin = 0.50;
  public double padmeBinBalanceMax = 2.00;

  public double keepRatio = 0.1;
  public List<Double> dataKeepRatios;

  public Integer maxStoredItems;
  public Integer maxRepresentatives;

  public int reportEvery = 1000;

  public int distBasePort = 9000;
  public int distCollectorPort = 9100;
  public long distSeed = 1337L;
  public int distGraceMs = 2000;
  public int distMaxRounds = 100;

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

    if (pssViewSize <= 0)
      throw new IllegalArgumentException("config.pssViewSize must be > 0");

    if (pssShuffleLength <= 0)
      throw new IllegalArgumentException("config.pssShuffleLength must be > 0");

    if (pssCycleEveryItems <= 0)
      throw new IllegalArgumentException("config.pssCycleEveryItems must be > 0");

    if (replFanout <= 0)
      throw new IllegalArgumentException("config.replFanout must be > 0");

    if (replBatchSize <= 0)
      throw new IllegalArgumentException("config.replBatchSize must be > 0");

    if (replCycleEveryItems <= 0)
      throw new IllegalArgumentException("config.replCycleEveryItems must be > 0");

    if (replTtl <= 0)
      throw new IllegalArgumentException("config.replTtl must be > 0");

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

    if (mode.equalsIgnoreCase("padme")) {
      if (maxStoredItems != null && maxStoredItems <= 0)
        throw new IllegalArgumentException("config.maxStoredItems must be > 0 when mode=padme");

      if (maxRepresentatives != null && maxRepresentatives <= 0)
        throw new IllegalArgumentException("config.maxRepresentatives must be > 0 when provided");

      if (maxStoredItems != null && maxRepresentatives != null && maxRepresentatives >= maxStoredItems)
        throw new IllegalArgumentException("config.maxRepresentatives must be < config.maxStoredItems when provided");

      if (refreshUtilitySpan < 0)
        throw new IllegalArgumentException("config.refreshUtilitySpan must be >= 0 when provided");
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