package padme.run;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import padme.retention.RandomRetentionPolicy;
import padme.retention.RepresentativeSet;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.InMemoryKvStore;
import padme.store.KvStore;

public final class Runner {
    private Runner() {}

    public static void run(Config cfg) {
        if (cfg.dataKeepRatios != null && cfg.dataKeepRatios.length > 0 && (cfg.mode.equalsIgnoreCase("padme") || cfg.mode.equalsIgnoreCase("random"))) {
            runSweep(cfg);
            return;
        }

        if (cfg.nodes != null && cfg.nodes > 1) runMultiNodeOnce(cfg, resolveOutDir(cfg.mode, null), null);
        else runSingleNodeOnce(cfg, resolveOutDir(cfg.mode, null), null);
    }

    private static void runSweep(Config base) {
        long totalRows = countInputRows(base.path, base.separator);
        for (double r : base.dataKeepRatios) {
            int ratioInt = toRatioInt(r);
            Config cfg = cloneConfig(base);
            cfg.keepRatio = r;
            cfg.maxStoredItems = computePerNodeBudget(totalRows, cfg.nodes, r);
            Path outDir = resolveOutDir(cfg.mode, ratioInt);
            runMultiNodeOnce(cfg, outDir, ratioInt);
        }
    }

    private static long countInputRows(String path, String sep) {
        long rows = 0;
        try (CsvRowReader rdr = new CsvRowReader(path, sep, true)) {
            rdr.header();
            while (rdr.nextRow() != null) rows++;
        } catch (Exception e) {
            throw new RuntimeException("Failed to count input rows for: " + path, e);
        }
        return rows;
    }

    private static void runSingleNodeOnce(Config cfg, Path outDir, Integer ratioInt) {
        Metrics m = new Metrics();
        RetentionPolicy policy = createPolicy(cfg, m, 0);

        Node node0 = createNode(0, policy);

        long startNs = System.nanoTime();

        String[] header;
        try (CsvRowReader rdr = new CsvRowReader(cfg.path, cfg.separator, true)) {
            header = rdr.header();
            VectorMapper mapper = NumericVectorMapper.fromHeader(header, cfg.idColumn, cfg.ignoreColumns);

            ingestLoopSingle(cfg, rdr, mapper, node0, m, policy, startNs);
        } catch (Exception e) {
            throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
        }

        System.out.println("DONE: " + cfg);
        System.out.printf(
                "Final: seen=%d stored=%d admitted=%d dropped=%d evicted=%d utilitySum=%.1f mode=%s%n",
                m.seen, node0.storedCount(), m.admitted, m.dropped, m.evicted, node0.totalUtility(), cfg.mode
        );

        writeOutputsSingle(cfg, outDir, header, node0);
    }

    private static void runMultiNodeOnce(Config cfg, Path outDir, Integer ratioInt) {
        int n = cfg.nodes;

        Metrics[] ms = new Metrics[n];
        Node[] nodes = new Node[n];

        for (int i = 0; i < n; i++) {
            ms[i] = new Metrics();
            nodes[i] = createNode(i, createPolicy(cfg, ms[i], i));
        }

        long seed = 1337L;
        PssOverlay overlay = new PssOverlay(n, cfg.pssViewSize, cfg.pssShuffleLength, seed);

        long startNs = System.nanoTime();

        String[] header;
        long rowsRead;
        try (CsvRowReader rdr = new CsvRowReader(cfg.path, cfg.separator, true)) {
            header = rdr.header();
            VectorMapper mapper = NumericVectorMapper.fromHeader(header, cfg.idColumn, cfg.ignoreColumns);

            rowsRead = ingestLoopMulti(cfg, rdr, mapper, nodes, ms, overlay, startNs);

            if (cfg.mode.equalsIgnoreCase("baseline")) {
                forceFullReplicationBaseline(nodes, ms);
                clearReplicationQueues(nodes);
                long elapsedNs = System.nanoTime() - startNs;
                printMultiProgress(cfg, nodes, ms, elapsedNs, rowsRead);
            } else {
                runBackgroundReplicationLoop(cfg, nodes, ms, overlay, startNs, rowsRead);
            }

        } catch (Exception e) {
            throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
        }

        printFinalMulti(cfg, nodes, ms);
        printFinalPerNode(cfg, nodes, ms);

        writeOutputsMulti(cfg, outDir, header, nodes);

        if (ratioInt != null) System.out.println("Wrote outputs: " + outDir.toAbsolutePath());
    }

    private static void ingestLoopSingle(Config cfg, CsvRowReader rdr, VectorMapper mapper, Node node, Metrics m, RetentionPolicy policy, long startNs) throws IOException {
        String[] row;
        long rowIdx = 0;

        while ((row = rdr.nextRow()) != null) {
            long key = computeKey(cfg, row, rowIdx);

            RetentionDecision d = ingestOne(node, mapper, cfg, key, row);
            m.seen++;
            m.record(d);

            double uMinStore = 0.0;
            int repsSize = 0;
            double repsMinU = 0.0;
            double repsMeanU = 0.0;

            if (policy instanceof PadmeRetentionPolicy p) {
                uMinStore = p.minUtilityStored();
                repsSize = p.representativeCount();
                repsMinU = p.repsMinUtility();
                repsMeanU = p.repsMeanUtility();
            }

            long elapsedNs = System.nanoTime() - startNs;
            m.maybePrint(cfg.reportEvery, node.storedCount(), node.storedBytes(), node.totalUtility(), elapsedNs, uMinStore, repsSize, repsMinU, repsMeanU);

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

        while (totalReplQueueSize(nodes) > 0) {
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
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long elapsedNs = System.nanoTime() - startNs;
        printMultiProgress(cfg, nodes, ms, elapsedNs, rowsRead);
    }

    private static void forceFullReplicationBaseline(Node[] nodes, Metrics[] ms) {
        List<Record> universe = unionByKeyPreferNewest(nodes);
        for (int i = 0; i < nodes.length; i++) {
            Metrics m = ms[i];
            for (Record r : universe) {
                RetentionDecision d = nodes[i].onRemoteRecord(r);
                m.seen++;
                m.record(d);
            }
        }
    }

    private static List<Record> unionByKeyPreferNewest(Node[] nodes) {
        Map<Long, Record> best = new HashMap<>();
        for (Node nd : nodes) {
            for (Record r : nd.snapshotRecords()) {
                if (r == null || r.meta == null) continue;
                Record cur = best.get(r.key);
                if (cur == null || cur.meta == null || r.meta.version > cur.meta.version) {
                    best.put(r.key, r);
                }
            }
        }
        return new ArrayList<>(best.values());
    }

    private static void clearReplicationQueues(Node[] nodes) {
        for (Node nd : nodes) {
            while (nd.replicationQueueSize() > 0) {
                nd.drainReplicationBatch(1024);
            }
        }
    }

    private static int totalReplQueueSize(Node[] nodes) {
        int total = 0;
        for (Node n : nodes) total += n.replicationQueueSize();
        return total;
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

    private static RetentionPolicy createPolicy(Config cfg, Metrics m, int nodeId) {
        if (cfg.mode.equalsIgnoreCase("baseline")) {
            return new BaselineFullRetentionPolicy();
        }
        if (cfg.mode.equalsIgnoreCase("random")) {
            long seed = 1337L ^ (((long) nodeId + 1L) * 0x9E3779B97F4A7C15L);
            return new RandomRetentionPolicy(cfg.maxStoredItems, seed);
        }
        RepresentativeSet reps = new RepresentativeSet(cfg.maxRepresentatives, new L2Distance());
        int refreshEveryItems = cfg.refreshUtilitySpan;
        return new PadmeRetentionPolicy(cfg.maxStoredItems, reps, refreshEveryItems, m);
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
                "Progress: rows=%d storedTotal=%d bytesTotal=%d utilityTotal=%.1f replQ=%d elapsed=%.2fs nodes=%d mode=%s%n%n",
                rowsRead, storedTotal, bytesTotal, utilityTotal, totalReplQueueSize(nodes), elapsedNs / 1e9, nodes.length, cfg.mode
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
                    "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f replQ=%d mode=%s%n",
                    i, m.seen, m.admitted, m.dropped, m.evicted, nd.storedCount(), nd.storedBytes(), nd.totalUtility(), nd.replicationQueueSize(), cfg.mode
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

    private static void writeOutputsSingle(Config cfg, Path outDir, String[] header, Node node0) {
        String prefix = cfg.mode.trim().toLowerCase();
        Path outNode = outDir.resolve(prefix + "_node0.csv");
        Path outMerged = outDir.resolve(prefix + "_merged.csv");

        try {
            RetainedDatasetWriter.writeSnapshotCsv(outNode, header, node0.snapshotRecords());
            RetainedDatasetWriter.writeSnapshotCsv(outMerged, header, node0.snapshotRecords());
            System.out.println("Wrote retained dataset: " + outNode.toAbsolutePath());
            System.out.println("Wrote retained dataset: " + outMerged.toAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write retained dataset snapshot CSV", e);
        }
    }

    private static void writeOutputsMulti(Config cfg, Path outDir, String[] header, Node[] nodes) {
        String prefix = cfg.mode.trim().toLowerCase();

        try {
            for (int i = 0; i < nodes.length; i++) {
                Path outNode = outDir.resolve(prefix + "_node" + i + ".csv");
                RetainedDatasetWriter.writeSnapshotCsv(outNode, header, nodes[i].snapshotRecords());
                System.out.println("Wrote retained dataset: " + outNode.toAbsolutePath());
            }

            List<Record> aggregated = aggregateSnapshotsKeepDuplicates(nodes);
            Path outMerged = outDir.resolve(prefix + "_merged.csv");
            RetainedDatasetWriter.writeSnapshotCsv(outMerged, header, aggregated);
            System.out.println("Wrote retained dataset: " + outMerged.toAbsolutePath());

        } catch (Exception e) {
            throw new RuntimeException("Failed to write retained dataset snapshot CSV", e);
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
                    "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f replQ=%d elapsed=%.2fs%n",
                    i, m.seen, m.admitted, m.dropped, m.evicted, nd.storedCount(), nd.storedBytes(), nd.totalUtility(), nd.replicationQueueSize(), elapsedNs / 1e9
            );
        }
    }

    private static int toRatioInt(double ratio) {
        return (int) Math.round(ratio * 100.0);
    }

    private static int computePerNodeBudget(long totalRows, int nodes, double ratio) {
        int local = (int) Math.ceil(totalRows / (double) nodes);
        int budget = (int) Math.ceil(local * ratio);
        if (budget <= 0) budget = 1;
        return budget;
    }

    private static Path resolveOutDir(String mode, Integer ratioInt) {
        String m = (mode == null || mode.isBlank()) ? "padme" : mode.trim().toLowerCase();
        Path base = Path.of("src/main/resources/data/output").resolve(m);
        if (ratioInt == null) return base;
        return base.resolve(Integer.toString(ratioInt));
    }

    private static Config cloneConfig(Config src) {
        Config c = new Config();
        c.path = src.path;
        c.separator = src.separator;
        c.idColumn = src.idColumn;

        c.mode = src.mode;
        c.nodes = src.nodes;

        c.pssViewSize = src.pssViewSize;
        c.pssShuffleLength = src.pssShuffleLength;
        c.pssCycleEveryItems = src.pssCycleEveryItems;

        c.replFanout = src.replFanout;
        c.replBatchSize = src.replBatchSize;
        c.replCycleEveryItems = src.replCycleEveryItems;

        c.dataKeepRatios = src.dataKeepRatios;
        c.keepRatio = src.keepRatio;

        c.maxStoredItems = src.maxStoredItems;
        c.maxRepresentatives = src.maxRepresentatives;
        c.refreshUtilitySpan = src.refreshUtilitySpan;

        c.reportEvery = src.reportEvery;

        c.ignoreColumns = src.ignoreColumns;

        return c;
    }
}