package padme.run;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import padme.config.Config;
import padme.data.CsvRowReader;
import padme.data.RetainedDatasetWriter;
import padme.feature.NumericVectorMapper;
import padme.feature.VectorMapper;
import padme.math.L2Distance;
import padme.metrics.Metrics;
import padme.model.DataItem;
import padme.model.Record;
import padme.node.Node;
import padme.pss.PssOverlay;
import padme.retention.BaselineFullRetentionPolicy;
import padme.retention.PadmeRetentionPolicy;
import padme.retention.RepresentativeSet;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.InMemoryKvStore;
import padme.store.KvStore;

public final class Runner {
  private Runner() {}

  public static void run(Config cfg) {
    if (cfg.nodes != null && cfg.nodes > 1) runMultiNode(cfg);
    else runSingleNode(cfg);
  }

  public static void runSingleNode(Config cfg) {
    Metrics m = new Metrics();
    RetentionPolicy policy = createPolicy(cfg, m);

    Node node0 = createNode(0, policy);

    long startNs = System.nanoTime();
    Path out = resolveOutPath(cfg);

    String[] header;
    try (CsvRowReader rdr = new CsvRowReader(cfg.path, cfg.separator, true)) {
      header = rdr.header();
      VectorMapper mapper = NumericVectorMapper.fromHeader(header, cfg.idColumn, cfg.ignoreColumns);

      ingestLoopSingle(cfg, rdr, mapper, node0, m, policy, startNs);
    } catch (Exception e) {
      throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
    }

    printFinal(cfg, node0, m);
    writeSnapshotOrThrow(out, header, node0);
  }

  public static void runMultiNode(Config cfg) {
    int n = cfg.nodes;

    Metrics[] ms = new Metrics[n];
    Node[] nodes = new Node[n];

    for (int i = 0; i < n; i++) {
      ms[i] = new Metrics();
      nodes[i] = createNode(i, createPolicy(cfg, ms[i]));
    }

    long seed = 1337L;
    PssOverlay overlay = new PssOverlay(n, cfg.pssViewSize, cfg.pssShuffleLength, seed);

    long startNs = System.nanoTime();
    Path out = resolveOutPath(cfg);

    String[] header;
    try (CsvRowReader rdr = new CsvRowReader(cfg.path, cfg.separator, true)) {
      header = rdr.header();
      VectorMapper mapper = NumericVectorMapper.fromHeader(header, cfg.idColumn, cfg.ignoreColumns);

      installShutdownHook(cfg, nodes, ms, header, out);

      long rowsRead = ingestLoopMulti(cfg, rdr, mapper, nodes, ms, overlay, startNs);

      runBackgroundReplicationLoop(cfg, nodes, ms, overlay, startNs, rowsRead);

    } catch (Exception e) {
      throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
    }
  }

  private static void ingestLoopSingle(Config cfg, CsvRowReader rdr, VectorMapper mapper, Node node, Metrics m, RetentionPolicy policy, long startNs) throws IOException {
    String[] row;
    long rowIdx = 0;

    while ((row = rdr.nextRow()) != null) {
      long key = computeKey(cfg, row, rowIdx);

      RetentionDecision d = ingestOne(node, mapper, cfg, key, row);
      m.seen++;
      m.record(d);

      PadmeStats ps = PadmeStats.from(policy);

      long elapsedNs = System.nanoTime() - startNs;
      m.maybePrint(cfg.reportEvery, node.storedCount(), node.storedBytes(), node.totalUtility(), elapsedNs, ps.uMinStore, ps.repsSize, ps.repsMinU, ps.repsMeanU);

      rowIdx++;
    }

  }

  private static long ingestLoopMulti(Config cfg, CsvRowReader rdr, VectorMapper mapper, Node[] nodes, Metrics[] ms, PssOverlay overlay, long startNs) throws IOException {
    int n = nodes.length;

    String[] row;
    long rowIdx = 0;

    while ((row = rdr.nextRow()) != null) {
      int owner = (int) (rowIdx % n);
      Node ownerNode = nodes[owner];
      Metrics m = ms[owner];

      long key = computeKey(cfg, row, rowIdx);
      RetentionDecision d = ingestOne(ownerNode, mapper, cfg, key, row);

      m.seen++;
      m.record(d);

      if (shouldCycle(rowIdx, cfg.pssCycleEveryItems)) overlay.cycleAll();
      if (shouldCycle(rowIdx, cfg.replCycleEveryItems)) replicationStep(nodes, overlay, cfg, ms);

      if (shouldCycle(rowIdx, cfg.reportEvery)) {
        long elapsedNs = System.nanoTime() - startNs;
        printMultiProgress(cfg, nodes, ms, elapsedNs, rowIdx + 1);
      }

      rowIdx++;
    }

    return rowIdx;
  }

  private static void runBackgroundReplicationLoop(Config cfg, Node[] nodes, Metrics[] ms, PssOverlay overlay, long startNs, long rowsRead) {
    final long reportEveryMs = resolveReportEveryMs(cfg);

    long lastReportNs = System.nanoTime();

    while (true) {
      overlay.cycleAll();
      replicationStep(nodes, overlay, cfg, ms);

      long nowNs = System.nanoTime();
      if (nowNs - lastReportNs >= reportEveryMs * 1_000_000L) {
        long elapsedNs = nowNs - startNs;
        printMultiProgress(cfg, nodes, ms, elapsedNs, rowsRead);
        System.out.flush();
        lastReportNs = nowNs;
      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private static long resolveReportEveryMs(Config cfg) {
    if (cfg.reportEvery != null && cfg.reportEvery > 0) {
      long ms = cfg.reportEvery;
      if (ms < 250) ms = 250;
      if (ms > 5000) ms = 5000;
      return ms;
    }
    return 1000;
  }

  private static boolean shouldCycle(long rowIdx, Integer every) {
    return every != null && every > 0 && (rowIdx + 1) % every == 0;
  }

  private static Node createNode(int id, RetentionPolicy policy) {
    KvStore kv = new InMemoryKvStore();
    return new Node(id, policy, kv);
  }

  private static RetentionPolicy createPolicy(Config cfg, Metrics m) {
    if (cfg.mode.equalsIgnoreCase("baseline")) {
      return new BaselineFullRetentionPolicy();
    }
    RepresentativeSet reps = new RepresentativeSet(cfg.maxRepresentatives, new L2Distance());
    int refreshEveryItems = cfg.refreshUtilitySpan;
    return new PadmeRetentionPolicy(cfg.maxStoredItems, reps, refreshEveryItems, m);
  }

  private static Path resolveOutPath(Config cfg) {
    return cfg.mode.equalsIgnoreCase("baseline") ? Path.of("src/main/resources/data/baseline_latest.csv") : Path.of("src/main/resources/data/padme_latest.csv");
  }

  private static RetentionDecision ingestOne(Node node, VectorMapper mapper, Config cfg, long key, String[] row) {
    DataItem item = new DataItem(row.clone());
    float[] vector = mapper.map(row, cfg.idColumn);
    return node.onLocalItem(key, item, vector);
  }

  private static long computeKey(Config cfg, String[] row, long fallback) {
    if (cfg.idColumn < 0 || cfg.idColumn >= row.length) return fallback;
    return parseKey(row[cfg.idColumn], fallback);
  }

  private static long parseKey(String s, long fallback) {
    try {
      String t = (s == null) ? "" : s.trim();
      if (t.isEmpty()) return fallback;
      return Long.parseLong(t);
    } catch (Exception ignored) {
      return fallback;
    }
  }

  private static final class PadmeStats {
    final double uMinStore;
    final int repsSize;
    final double repsMinU;
    final double repsMeanU;

    private PadmeStats(double uMinStore, int repsSize, double repsMinU, double repsMeanU) {
      this.uMinStore = uMinStore;
      this.repsSize = repsSize;
      this.repsMinU = repsMinU;
      this.repsMeanU = repsMeanU;
    }

    static PadmeStats from(RetentionPolicy policy) {
      if (policy instanceof PadmeRetentionPolicy p) {
        return new PadmeStats(
                p.minUtilityStored(),
                p.representativeCount(),
                p.repsMinUtility(),
                p.repsMeanUtility()
        );
      }
      return new PadmeStats(0.0, 0, 0.0, 0.0);
    }
  }

  private static void printFinal(Config cfg, Node node0, Metrics m) {
    System.out.println("DONE: " + cfg);
    System.out.printf(
            "Final: seen=%d stored=%d admitted=%d dropped=%d evicted=%d utilitySum=%.1f mode=%s%n",
            m.seen, node0.storedCount(), m.admitted, m.dropped, m.evicted, node0.totalUtility(), cfg.mode
    );
  }

  private static void installShutdownHook(Config cfg, Node[] nodes, Metrics[] ms, String[] header, Path out) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printFinalMulti(cfg, nodes, ms);
        printFinalPerNode(cfg, nodes, ms);
        List<Record> aggregated = aggregateSnapshotsKeepDuplicates(nodes);
        RetainedDatasetWriter.writeSnapshotCsv(out, header, aggregated);
        System.out.println("Wrote retained dataset: " + out.toAbsolutePath());
      } catch (Exception ignored) {
      }
    }, "padme-shutdown-hook"));
  }

  private static void writeSnapshotOrThrow(Path out, String[] header, Node node0) {
    try {
      RetainedDatasetWriter.writeSnapshotCsv(out, header, node0.snapshotRecords());
      System.out.println("Wrote retained dataset: " + out.toAbsolutePath());
    } catch (Exception e) {
      throw new RuntimeException("Failed to write retained dataset snapshot CSV", e);
    }
  }

  private static String[] headerFromNodeCsv(Config cfg) {
    throw new UnsupportedOperationException("Pass the CSV header you already read, do not call this.");
  }

  private static void printMultiProgress(Config cfg, Node[] nodes, Metrics[] ms, long elapsedNs, long rowsRead) {
    int storedTotal = 0;
    long bytesTotal = 0L;
    double utilityTotal = 0.0;

    for (Node nd : nodes) {
      storedTotal += nd.storedCount();
      bytesTotal += nd.storedBytes();
      utilityTotal += nd.totalUtility();
    }

    System.out.printf(
            "Progress: rows=%d storedTotal=%d bytesTotal=%d utilityTotal=%.1f elapsed=%.2fs nodes=%d mode=%s%n%n",
            rowsRead, storedTotal, bytesTotal, utilityTotal, elapsedNs / 1e9, nodes.length, cfg.mode
    );

    printPerNodeProgress(nodes, ms, elapsedNs);
  }

  private static void printFinalMulti(Config cfg, Node[] nodes, Metrics[] ms) {
    long seen = 0;
    long admitted = 0;
    long dropped = 0;
    long evicted = 0;
    int stored = 0;
    double util = 0.0;

    for (int i = 0; i < nodes.length; i++) {
      Metrics m = ms[i];
      seen += m.seen;
      admitted += m.admitted;
      dropped += m.dropped;
      evicted += m.evicted;
      stored += nodes[i].storedCount();
      util += nodes[i].totalUtility();
    }

    System.out.println("DONE: " + cfg);
    System.out.printf(
            "Final (multi): seen=%d storedTotal=%d admitted=%d dropped=%d evicted=%d utilitySum=%.1f nodes=%d mode=%s%n",
            seen, stored, admitted, dropped, evicted, util, nodes.length, cfg.mode
    );
  }

  private static void printFinalPerNode(Config cfg, Node[] nodes, Metrics[] ms) {
    System.out.println("Final per node:");
    for (int i = 0; i < nodes.length; i++) {
      Node nd = nodes[i];
      Metrics m = ms[i];
      System.out.printf(
              "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f mode=%s%n",
              i, m.seen, m.admitted, m.dropped, m.evicted, nd.storedCount(), nd.storedBytes(), nd.totalUtility(), cfg.mode
      );
    }
  }

  private static void replicationStep(Node[] nodes, PssOverlay overlay, Config cfg, Metrics[] ms) {
    int n = nodes.length;
    int fanout = Math.max(1, Math.min(cfg.replFanout, Math.max(1, n - 1)));
    int batchSize = Math.max(1, cfg.replBatchSize);

    for (int i = 0; i < n; i++) {
      List<Record> batch = nodes[i].drainReplicationBatch(batchSize);
      if (batch.isEmpty()) continue;

      int[] peers = overlay.samplePeers(i, fanout);
      for (int p : peers) {
        if (p < 0 || p >= n || p == i) continue;

        Metrics m = ms[p];
        for (Record r : batch) {
          RetentionDecision d = nodes[p].onRemoteRecord(r);
          m.seen++;
          m.record(d);
        }
      }
    }
  }

  private static List<Record> aggregateSnapshotsKeepDuplicates(Node[] nodes) {
    int total = 0;
    for (Node nd : nodes) total += nd.storedCount();

    List<Record> out = new ArrayList<>(Math.max(16, total));
    for (Node nd : nodes) {
      for (Record r : nd.snapshotRecords()) out.add(r);
    }
    return out;
  }

  private static void printPerNodeProgress(Node[] nodes, Metrics[] ms, long elapsedNs) {
    for (int i = 0; i < nodes.length; i++) {
      Node nd = nodes[i];
      Metrics m = ms[i];
      System.out.printf(
              "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f elapsed=%.2fs%n",
              i, m.seen, m.admitted, m.dropped, m.evicted, nd.storedCount(), nd.storedBytes(), nd.totalUtility(), elapsedNs / 1e9
      );
    }
  }
}