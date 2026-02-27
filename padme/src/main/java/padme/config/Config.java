package padme.config;

import java.util.Arrays;

public final class Config {
  public String path;
  public String separator = ",";
  public int idColumn = -1;

  public String mode;
  public Integer nodes;

  public Integer pssViewSize;
  public Integer pssShuffleLength;
  public Integer pssCycleEveryItems;

  public Integer replFanout;
  public Integer replBatchSize;
  public Integer replCycleEveryItems;

  public Integer maxStoredItems;
  public Integer maxRepresentatives;
  public Integer refreshUtilitySpan;

  public Integer reportEvery;

  public Integer distBasePort;
  public Integer distCollectorPort;
  public Long distSeed;
  public Integer distGraceMs;
  public Integer distMaxRounds;
  public String distTmpDir;

  public String[] ignoreColumns = new String[0];

  public void validate() {
    if (path == null || path.isBlank())
      throw new IllegalArgumentException("config.path is required and must be non-empty");

    if (mode == null || (!mode.equalsIgnoreCase("padme") && !mode.equalsIgnoreCase("baseline")))
      throw new IllegalArgumentException("config.mode must be 'padme' or 'baseline'");

    if (nodes == null || nodes <= 0)
      throw new IllegalArgumentException("config.nodes must be > 0");

    if (pssViewSize == null) pssViewSize = Math.max(1, Math.min(8, nodes - 1));
    if (pssShuffleLength == null) pssShuffleLength = Math.max(1, Math.min(pssViewSize, 4));
    if (pssCycleEveryItems == null || pssCycleEveryItems <= 0) pssCycleEveryItems = 200;

    if (replFanout == null) replFanout = Math.max(1, Math.min(2, nodes - 1));
    if (replBatchSize == null || replBatchSize <= 0) replBatchSize = 32;
    if (replCycleEveryItems == null || replCycleEveryItems <= 0) replCycleEveryItems = 200;

    if (reportEvery == null || reportEvery <= 0)
      throw new IllegalArgumentException("config.reportEvery must be > 0");

    if (distBasePort == null || distBasePort <= 0) distBasePort = 9000;
    if (distCollectorPort == null || distCollectorPort <= 0) distCollectorPort = 9100;
    if (distSeed == null) distSeed = 1337L;
    if (distGraceMs == null || distGraceMs < 0) distGraceMs = 2000;
    if (distMaxRounds == null || distMaxRounds <= 0) distMaxRounds = 10_000;
    if (distTmpDir == null) distTmpDir = "";

    if (ignoreColumns == null) ignoreColumns = new String[0];

    for (int i = 0; i < ignoreColumns.length; i++) {
      String s = ignoreColumns[i];
      ignoreColumns[i] = (s == null) ? "" : s.trim();
    }

    ignoreColumns = Arrays.stream(ignoreColumns)
            .filter(s -> s != null && !s.isBlank())
            .distinct()
            .toArray(String[]::new);

    if (mode.equalsIgnoreCase("padme")) {
      if (maxStoredItems == null || maxStoredItems <= 0)
        throw new IllegalArgumentException("config.maxStoredItems must be > 0 when mode=padme");

      if (maxRepresentatives == null || maxRepresentatives <= 0)
        throw new IllegalArgumentException("config.maxRepresentatives must be > 0 when mode=padme");

      if (refreshUtilitySpan == null || refreshUtilitySpan <= 0)
        throw new IllegalArgumentException("config.refreshUtilitySpan must be > 0 when mode=padme");
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
            ", maxStoredItems=" + maxStoredItems +
            ", maxRepresentatives=" + maxRepresentatives +
            ", reportEvery=" + reportEvery +
            ", distBasePort=" + distBasePort +
            ", distCollectorPort=" + distCollectorPort +
            ", distSeed=" + distSeed +
            ", distGraceMs=" + distGraceMs +
            ", distMaxRounds=" + distMaxRounds +
            ", distTmpDir='" + distTmpDir + '\'' +
            ", ignoreColumns=" + Arrays.toString(ignoreColumns) +
            '}';
  }
}