package padme.run;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

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
import padme.retention.GraphCutRetentionPolicy;
import padme.retention.KCenterRetentionPolicy;
import padme.retention.RandomRetentionPolicy;
import padme.retention.RepresentativeSet;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.InMemoryKvStore;
import padme.store.KvStore;

public final class Runner {
    private Runner() {}

    public static void run(Config cfg) {
        assignDerivedOverlayParams(cfg);

        String mode = normalizeMode(cfg.mode);
        if (cfg.dataKeepRatios != null && !cfg.dataKeepRatios.isEmpty() &&
                (mode.equals("padme")
                        || mode.equals("random")
                        || mode.equals("graph_cut")
                        || mode.equals("max_diversity")
                        || mode.equals("k_center"))) {
            runSweep(cfg);
            return;
        }

        if (usesRepresentatives(mode)) {
            assignDerivedRepresentativeParams(cfg);
        }

        cfg.validate();
        if (cfg.nodes > 1) runMultiNodeOnce(cfg, resolveOutDir(cfg, mode, null), null);
        else runSingleNodeOnce(cfg, resolveOutDir(cfg, mode, null));
    }

    private static void runSweep(Config base) {
        long totalRows = countInputRows(base.path, base.separator);

        for (double ratio : base.dataKeepRatios) {
            int ratioInt = toRatioInt(ratio);
            Config cfg = cloneConfig(base);
            cfg.keepRatio = ratio;
            cfg.maxStoredItems = computePerNodeBudget(totalRows, ratio);

            assignDerivedOverlayParams(cfg);

            if (usesRepresentatives(normalizeMode(cfg.mode))) {
                assignDerivedRepresentativeParams(cfg);
            }

            cfg.validate();

            Path outDir = resolveOutDir(cfg, normalizeMode(cfg.mode), ratioInt);
            runMultiNodeOnce(cfg, outDir, ratioInt);
        }
    }

    private static long countInputRows(String path, String separator) {
        long rows = 0;
        try (CsvRowReader reader = new CsvRowReader(path, separator, true)) {
            reader.header();
            while (reader.nextRow() != null) rows++;
        } catch (Exception e) {
            throw new RuntimeException("Failed to count input rows for: " + path, e);
        }
        return rows;
    }

    private static void runSingleNodeOnce(Config cfg, Path outDir) {
        Metrics metrics = new Metrics();
        RetentionPolicy policy = createPolicy(cfg, metrics, 0);
        Node node0 = createNode(0, cfg, policy);

        long startNs = System.nanoTime();

        String[] header;
        try (CsvRowReader reader = new CsvRowReader(cfg.path, cfg.separator, true)) {
            header = reader.header();
            List<String[]> fitRows = loadRowsForVectorFit(cfg, 20_000);
            VectorMapper mapper = NumericVectorMapper.fit(
                    header,
                    cfg.idColumn,
                    cfg.ignoreColumns,
                    fitRows,
                    cfg.vectorTransform
            );

            ingestLoopSingle(cfg, reader, mapper, node0, metrics, policy, startNs);
        } catch (Exception e) {
            throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
        }

        long elapsedNs = System.nanoTime() - startNs;
        double elapsedSeconds = elapsedNs / 1_000_000_000.0;

        System.out.println("DONE: " + cfg);
        System.out.printf(
                "Final: seen=%d stored=%d admitted=%d dropped=%d evicted=%d utilitySum=%.1f mode=%s simTime=%.3fs%n",
                metrics.seen,
                node0.storedCount(),
                metrics.admitted,
                metrics.dropped,
                metrics.evicted,
                node0.totalUtility(),
                normalizeMode(cfg.mode),
                elapsedSeconds
        );

        writeOutputsSingle(cfg, outDir, header, node0);
        writeMetricsJson(cfg, outDir, metrics.totalBytesSent, elapsedSeconds, new Node[]{node0});
    }

    private static void runMultiNodeOnce(Config cfg, Path outDir, Integer ratioInt) {
        int nodeCount = cfg.nodes;

        Metrics[] metrics = new Metrics[nodeCount];
        Node[] nodes = new Node[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            metrics[i] = new Metrics();
            nodes[i] = createNode(i, cfg, createPolicy(cfg, metrics[i], i));
        }

        long seed = 1337L;
        PssOverlay overlay = new PssOverlay(
                nodeCount,
                cfg.pssViewSize,
                cfg.pssShuffleLength,
                seed
        );

        long startNs = System.nanoTime();

        String[] header;
        long rowsRead;
        try (CsvRowReader reader = new CsvRowReader(cfg.path, cfg.separator, true)) {
            header = reader.header();
            List<String[]> fitRows = loadRowsForVectorFit(cfg, 20_000);
            VectorMapper mapper = NumericVectorMapper.fit(
                    header,
                    cfg.idColumn,
                    cfg.ignoreColumns,
                    fitRows,
                    cfg.vectorTransform
            );

            rowsRead = ingestLoopMulti(cfg, reader, mapper, nodes, metrics, overlay, startNs);
            runBackgroundReplicationLoop(cfg, nodes, metrics, overlay, startNs, rowsRead);
        } catch (Exception e) {
            throw new RuntimeException("Ingest failed while reading: " + cfg.path, e);
        }

        long elapsedNs = System.nanoTime() - startNs;
        double elapsedSeconds = elapsedNs / 1_000_000_000.0;

        printFinalMulti(cfg, nodes, metrics);
        printFinalPerNode(cfg, nodes, metrics);

        writeOutputsMulti(cfg, outDir, header, nodes);
        writeMetricsJson(cfg, outDir, totalBytesSent(metrics), elapsedSeconds, nodes);

        if (ratioInt != null) {
            System.out.println("Wrote outputs: " + outDir.toAbsolutePath());
        }
    }

    private static List<String[]> loadRowsForVectorFit(Config cfg, int maxRows) throws IOException {
        List<String[]> rows = new ArrayList<>();

        try (CsvRowReader reader = new CsvRowReader(cfg.path, cfg.separator, true)) {
            reader.header();
            String[] row;
            while ((row = reader.nextRow()) != null && rows.size() < maxRows) {
                rows.add(row.clone());
            }
        }

        return rows;
    }

    private static void ingestLoopSingle(
            Config cfg,
            CsvRowReader reader,
            VectorMapper mapper,
            Node node,
            Metrics metrics,
            RetentionPolicy policy,
            long startNs
    ) throws IOException {
        String[] row;
        long rowIdx = 0;

        while ((row = reader.nextRow()) != null) {
            long key = computeKey(cfg, row, rowIdx);

            RetentionDecision decision = ingestOne(node, mapper, cfg, key, row);
            metrics.seen++;
            metrics.record(decision);

            double minStoredUtility = 0.0;
            int representativeCount = 0;
            double minRepresentativeUtility = 0.0;
            double meanRepresentativeUtility = 0.0;

            if (policy instanceof GraphCutRetentionPolicy graphCut) {
                representativeCount = graphCut.representativeCount();
                minRepresentativeUtility = graphCut.repsMinUtility();
                meanRepresentativeUtility = graphCut.repsMeanUtility();
                minStoredUtility = graphCut.minUtilityStored();
            } else if (policy instanceof KCenterRetentionPolicy kCenter) {
                representativeCount = kCenter.representativeCount();
                minRepresentativeUtility = kCenter.repsMinUtility();
                meanRepresentativeUtility = kCenter.repsMeanUtility();
                minStoredUtility = kCenter.minUtilityStored();
            }

            long elapsedNs = System.nanoTime() - startNs;
            metrics.maybePrint(
                    cfg.reportEvery,
                    node.storedCount(),
                    node.storedBytes(),
                    node.totalUtility(),
                    elapsedNs,
                    minStoredUtility,
                    representativeCount,
                    minRepresentativeUtility,
                    meanRepresentativeUtility
            );

            rowIdx++;
        }
    }

    private static long ingestLoopMulti(
            Config cfg,
            CsvRowReader reader,
            VectorMapper mapper,
            Node[] nodes,
            Metrics[] metrics,
            PssOverlay overlay,
            long startNs
    ) throws IOException {
        int nodeCount = nodes.length;

        String[] row;
        long rowIdx = 0;
        while ((row = reader.nextRow()) != null) {
            int owner = (int) (rowIdx % nodeCount);
            Node ownerNode = nodes[owner];
            Metrics ownerMetrics = metrics[owner];

            long key = computeKey(cfg, row, rowIdx);
            RetentionDecision decision = ingestOne(ownerNode, mapper, cfg, key, row);

            ownerMetrics.seen++;
            ownerMetrics.record(decision);

            if (shouldCycle(rowIdx, cfg.pssCycleEveryItems)) {
                overlay.cycleAll();
            }
            if (shouldCycle(rowIdx, cfg.replCycleEveryItems)) {
                replicationStep(nodes, overlay, cfg, metrics);
            }

            if (shouldCycle(rowIdx, cfg.reportEvery)) {
                long elapsedNs = System.nanoTime() - startNs;
                printMultiProgress(cfg, nodes, metrics, elapsedNs, rowIdx + 1);
            }

            rowIdx++;
        }

        return rowIdx;
    }

    private static void runBackgroundReplicationLoop(
            Config cfg,
            Node[] nodes,
            Metrics[] metrics,
            PssOverlay overlay,
            long startNs,
            long rowsRead
    ) {
        final long reportEveryMs = resolveReportEveryMs(cfg);
        long lastReportNs = System.nanoTime();

        while (totalReplQueueSize(nodes) > 0) {
            overlay.cycleAll();
            replicationStep(nodes, overlay, cfg, metrics);

            long nowNs = System.nanoTime();
            if (nowNs - lastReportNs >= reportEveryMs * 1_000_000L) {
                long elapsedNs = nowNs - startNs;
                printMultiProgress(cfg, nodes, metrics, elapsedNs, rowsRead);
                System.out.flush();
                lastReportNs = nowNs;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long elapsedNs = System.nanoTime() - startNs;
        printMultiProgress(cfg, nodes, metrics, elapsedNs, rowsRead);
    }

    private static int totalReplQueueSize(Node[] nodes) {
        int total = 0;
        for (Node node : nodes) total += node.replicationQueueSize();
        return total;
    }

    private static long totalBytesSent(Metrics[] metrics) {
        long total = 0L;
        for (Metrics nodeMetrics : metrics) total += nodeMetrics.totalBytesSent;
        return total;
    }

    private static long resolveReportEveryMs(Config cfg) {
        if (cfg.reportEvery > 0) {
            long milliseconds = cfg.reportEvery;
            if (milliseconds < 250) milliseconds = 250;
            if (milliseconds > 5_000) milliseconds = 5_000;
            return milliseconds;
        }
        return 1_000;
    }

    private static boolean shouldCycle(long rowIdx, Integer every) {
        return every != null && every > 0 && (rowIdx + 1) % every == 0;
    }

    private static Node createNode(int id, Config cfg, RetentionPolicy policy) {
        KvStore kvStore = new InMemoryKvStore();
        return new Node(id, policy, kvStore, cfg.replTtl, cfg.replSeenCacheSize);
    }

    private static RetentionPolicy createPolicy(Config cfg, Metrics metrics, int nodeId) {
        String mode = normalizeMode(cfg.mode);
        if (mode.equals("baseline")) {
            return new BaselineFullRetentionPolicy();
        }

        if (mode.equals("random")) {
            long seed = 1337L ^ (((long) nodeId + 1L) * 0x9E3779B97F4A7C15L);
            return new RandomRetentionPolicy(cfg.maxStoredItems, seed);
        }

        int maxStored = cfg.maxStoredItems;
        int maxRepresentatives = cfg.maxRepresentatives;
        if (maxStored <= 0) {
            throw new IllegalArgumentException("maxStoredItems must be > 0 for mode=" + mode);
        }

        if (maxRepresentatives <= 0) {
            throw new IllegalArgumentException("maxRepresentatives must be > 0 for mode=" + mode);
        }

        if (maxRepresentatives >= maxStored) {
            maxRepresentatives = Math.max(1, maxStored - 1);
        }

        RepresentativeSet representatives = new RepresentativeSet(maxRepresentatives, new L2Distance());
        int refreshEveryItems = cfg.refreshUtilitySpan;

        if (mode.equals("graph_cut")) {
            return new GraphCutRetentionPolicy(
                    maxStored,
                    representatives,
                    refreshEveryItems,
                    2.0,
                    cfg.nonRepSampleFactor,
                    metrics
            );
        }

        if (mode.equals("k_center")) {
            return new KCenterRetentionPolicy(
                    maxStored,
                    representatives,
                    refreshEveryItems,
                    metrics
            );
        }

        throw new IllegalArgumentException("Unsupported mode: " + cfg.mode);
    }

    private static RetentionDecision ingestOne(
            Node node,
            VectorMapper mapper,
            Config cfg,
            long key,
            String[] row
    ) {
        DataItem item = new DataItem(row.clone());
        float[] vector = mapper.map(row, cfg.idColumn);
        return node.onLocalItem(key, item, vector);
    }

    private static long computeKey(Config cfg, String[] row, long fallback) {
        if (cfg.idColumn < 0 || cfg.idColumn >= row.length) return fallback;
        return parseKey(row[cfg.idColumn], fallback);
    }

    private static long parseKey(String value, long fallback) {
        try {
            String trimmed = (value == null) ? "" : value.trim();
            if (trimmed.isEmpty()) return fallback;
            return Long.parseLong(trimmed);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static void printMultiProgress(
            Config cfg,
            Node[] nodes,
            Metrics[] metrics,
            long elapsedNs,
            long rowsRead
    ) {
        int storedTotal = 0;
        long bytesTotal = 0L;
        double utilityTotal = 0.0;

        for (Node node : nodes) {
            storedTotal += node.storedCount();
            bytesTotal += node.storedBytes();
            utilityTotal += node.totalUtility();
        }

        System.out.printf(
                "Progress: rows=%d storedTotal=%d bytesTotal=%d utilityTotal=%.1f replQ=%d elapsed=%.2fs nodes=%d mode=%s%n%n",
                rowsRead,
                storedTotal,
                bytesTotal,
                utilityTotal,
                totalReplQueueSize(nodes),
                elapsedNs / 1e9,
                nodes.length,
                normalizeMode(cfg.mode)
        );

        printPerNodeProgress(nodes, metrics, elapsedNs);
    }

    private static void printFinalMulti(Config cfg, Node[] nodes, Metrics[] metrics) {
        long seen = 0;
        long admitted = 0;
        long dropped = 0;
        long evicted = 0;
        int stored = 0;
        double utility = 0.0;

        for (int i = 0; i < nodes.length; i++) {
            Metrics nodeMetrics = metrics[i];
            seen += nodeMetrics.seen;
            admitted += nodeMetrics.admitted;
            dropped += nodeMetrics.dropped;
            evicted += nodeMetrics.evicted;
            stored += nodes[i].storedCount();
            utility += nodes[i].totalUtility();
        }

        StorageSummary storage = summarizeStorage(cfg, nodes);

        System.out.println("DONE: " + cfg);
        System.out.printf(
                "Final (multi): seen=%d storedTotal=%d admitted=%d dropped=%d evicted=%d utilitySum=%.1f nodes=%d mode=%s " +
                        "storedPerNode[min=%d mean=%.2f max=%d] underfilledNodes=%d%n",
                seen,
                stored,
                admitted,
                dropped,
                evicted,
                utility,
                nodes.length,
                normalizeMode(cfg.mode),
                storage.minStored,
                storage.meanStored,
                storage.maxStored,
                storage.underfilledNodes
        );

        if (storage.underfilledNodes > 0) {
            System.out.printf(
                    "WARNING: %d/%d nodes finished below the configured retention budget. " +
                            "This is an expected possible outcome of finite push-gossip without repair.%n",
                    storage.underfilledNodes,
                    nodes.length
            );
        }
    }

    private static void printFinalPerNode(Config cfg, Node[] nodes, Metrics[] metrics) {
        System.out.println("Final per node:");
        for (int i = 0; i < nodes.length; i++) {
            Node node = nodes[i];
            Metrics nodeMetrics = metrics[i];
            System.out.printf(
                    "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f replQ=%d mode=%s%n",
                    i,
                    nodeMetrics.seen,
                    nodeMetrics.admitted,
                    nodeMetrics.dropped,
                    nodeMetrics.evicted,
                    node.storedCount(),
                    node.storedBytes(),
                    node.totalUtility(),
                    node.replicationQueueSize(),
                    normalizeMode(cfg.mode)
            );
        }
    }

    private static void replicationStep(
            Node[] nodes,
            PssOverlay overlay,
            Config cfg,
            Metrics[] metrics
    ) {
        int nodeCount = nodes.length;
        int fanout = Math.max(1, Math.min(cfg.replFanout, Math.max(1, nodeCount - 1)));
        int batchSize = Math.max(1, cfg.replBatchSize);

        for (int senderId = 0; senderId < nodeCount; senderId++) {
            List<Node.QueuedRecord> batch = nodes[senderId].drainReplicationBatch(batchSize);
            if (batch.isEmpty()) continue;

            for (Node.QueuedRecord queued : batch) {
                int[] peers = overlay.samplePeers(senderId, fanout);
                for (int receiverId : peers) {
                    if (receiverId < 0 || receiverId >= nodeCount || receiverId == senderId) continue;

                    Record record = queued.record;
                    metrics[senderId].totalBytesSent += computeRecordPayloadBytes(record);

                    RetentionDecision decision = nodes[receiverId].onRemoteRecord(
                            record,
                            queued.ttlRemaining
                    );
                    metrics[receiverId].seen++;
                    metrics[receiverId].record(decision);
                }
            }
        }
    }

    private static long computeRecordPayloadBytes(Record record) {
        if (record == null) return 0L;

        long bytes = Long.BYTES;

        if (record.meta != null) {
            bytes += Long.BYTES;
            bytes += Double.BYTES;
        }

        if (record.item != null && record.item.fields != null) {
            for (String field : record.item.fields) {
                if (field != null) {
                    bytes += field.getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }

        return bytes;
    }

    private static void writeOutputsSingle(Config cfg, Path outDir, String[] header, Node node0) {
        String prefix = normalizeMode(cfg.mode);
        Path outNode = outDir.resolve(prefix + "_node0.csv");

        try {
            RetainedDatasetWriter.writeSnapshotCsv(outNode, header, node0.snapshotRecords());
            System.out.println("Wrote retained dataset: " + outNode.toAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write retained dataset snapshot CSV", e);
        }
    }

    private static void writeOutputsMulti(Config cfg, Path outDir, String[] header, Node[] nodes) {
        String prefix = normalizeMode(cfg.mode);

        try {
            for (int i = 0; i < nodes.length; i++) {
                Path outNode = outDir.resolve(prefix + "_node" + i + ".csv");
                RetainedDatasetWriter.writeSnapshotCsv(outNode, header, nodes[i].snapshotRecords());
                System.out.println("Wrote retained dataset: " + outNode.toAbsolutePath());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write retained dataset snapshot CSV", e);
        }
    }

    private static void writeMetricsJson(
            Config cfg,
            Path outDir,
            long totalBytesSent,
            double simulationTimeSeconds,
            Node[] nodes
    ) {
        try {
            java.nio.file.Files.createDirectories(outDir);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> root = new LinkedHashMap<>();
            StorageSummary storage = summarizeStorage(cfg, nodes);

            root.put("dataset", resolveDatasetKey(cfg));
            root.put("mode", normalizeMode(cfg.mode));
            if (!normalizeMode(cfg.mode).equals("baseline")) {
                root.put("keepRatio", cfg.keepRatio);
            }
            root.put("nodes", cfg.nodes);
            root.put("pssViewSize", cfg.pssViewSize);
            root.put("pssShuffleLength", cfg.pssShuffleLength);
            root.put("replFanout", cfg.replFanout);
            root.put("replTtl", cfg.replTtl);
            root.put("replSeenCacheSize", cfg.replSeenCacheSize);
            root.put("totalBytesSent", totalBytesSent);
            root.put("simulationTimeSeconds", simulationTimeSeconds);
            root.put("minStoredItemsPerNode", storage.minStored);
            root.put("meanStoredItemsPerNode", storage.meanStored);
            root.put("maxStoredItemsPerNode", storage.maxStored);
            root.put("underfilledNodes", storage.underfilledNodes);
            if (cfg.maxStoredItems != null) {
                root.put("targetStoredItemsPerNode", cfg.maxStoredItems);
            }

            Path out = outDir.resolve("metrics.json");
            mapper.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), root);

            System.out.println("Wrote metrics JSON: " + out.toAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write metrics.json", e);
        }
    }

    private static void printPerNodeProgress(Node[] nodes, Metrics[] metrics, long elapsedNs) {
        for (int i = 0; i < nodes.length; i++) {
            Node node = nodes[i];
            Metrics nodeMetrics = metrics[i];
            System.out.printf(
                    "N%d seen=%d admitted=%d dropped=%d evicted=%d stored=%d bytes=%d utility=%.1f replQ=%d elapsed=%.2fs%n",
                    i,
                    nodeMetrics.seen,
                    nodeMetrics.admitted,
                    nodeMetrics.dropped,
                    nodeMetrics.evicted,
                    node.storedCount(),
                    node.storedBytes(),
                    node.totalUtility(),
                    node.replicationQueueSize(),
                    elapsedNs / 1e9
            );
        }
    }

    private static StorageSummary summarizeStorage(Config cfg, Node[] nodes) {
        if (nodes == null || nodes.length == 0) {
            return new StorageSummary(0, 0.0, 0, 0);
        }

        int minStored = Integer.MAX_VALUE;
        int maxStored = Integer.MIN_VALUE;
        long totalStored = 0L;
        int underfilledNodes = 0;

        boolean hasTarget = !normalizeMode(cfg.mode).equals("baseline")
                && cfg.maxStoredItems != null
                && cfg.maxStoredItems > 0;

        for (Node node : nodes) {
            int stored = node.storedCount();
            minStored = Math.min(minStored, stored);
            maxStored = Math.max(maxStored, stored);
            totalStored += stored;

            if (hasTarget && stored < cfg.maxStoredItems) {
                underfilledNodes++;
            }
        }

        return new StorageSummary(
                minStored,
                (double) totalStored / nodes.length,
                maxStored,
                underfilledNodes
        );
    }

    private static int toRatioInt(double ratio) {
        return (int) Math.round(ratio * 100.0);
    }

    private static int computePerNodeBudget(long totalRows, double ratio) {
        int budget = (int) Math.ceil(totalRows * ratio);
        if (budget <= 0) budget = 1;
        return budget;
    }

    private static Path resolveOutDir(Config cfg, String mode, Integer ratioInt) {
        String normalizedMode = normalizeMode(mode);
        Path base = resolveDatasetOutRoot(cfg).resolve(normalizedMode);
        if (ratioInt == null) return base;
        return base.resolve(Integer.toString(ratioInt));
    }

    private static Path resolveDatasetOutRoot(Config cfg) {
        return Path.of("src/main/resources/data/output").resolve(resolveDatasetKey(cfg));
    }

    private static String resolveDatasetKey(Config cfg) {
        String fileName = Path.of(cfg.path).getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String stem = dot >= 0 ? fileName.substring(0, dot) : fileName;
        String lower = stem.toLowerCase();

        if (lower.endsWith("_train")) {
            lower = lower.substring(0, lower.length() - 6);
        }

        return switch (lower) {
            case "creditcard" -> "credit_card";
            case "foresttype" -> "forest_type";
            default -> lower;
        };
    }

    private static void assignDerivedRepresentativeParams(Config cfg) {
        if (cfg.maxStoredItems != null && cfg.maxStoredItems > 0) {
            if (cfg.maxRepresentatives == null || cfg.maxRepresentatives <= 0) {
                int derived = (int) Math.floor(Math.sqrt(cfg.maxStoredItems));
                derived = Math.max(1, derived);
                if (derived >= cfg.maxStoredItems) {
                    derived = Math.max(1, cfg.maxStoredItems - 1);
                }
                cfg.maxRepresentatives = derived;
            }

            if (cfg.maxRepresentatives >= cfg.maxStoredItems) {
                cfg.maxRepresentatives = Math.max(1, cfg.maxStoredItems - 1);
            }
        }
    }

    private static void assignDerivedOverlayParams(Config cfg) {
        int nodeCount = Math.max(1, cfg.nodes);
        int maxPeers = Math.max(1, nodeCount - 1);
        int logN = ceilLog2(nodeCount);

        if (cfg.pssViewSize == null || cfg.pssViewSize <= 0) {
            int derivedViewSize = Math.max(4, 2 * logN);
            cfg.pssViewSize = Math.min(maxPeers, derivedViewSize);
        } else {
            cfg.pssViewSize = Math.min(cfg.pssViewSize, maxPeers);
        }

        if (cfg.pssShuffleLength == null || cfg.pssShuffleLength <= 0) {
            int derivedShuffleLength = Math.max(2, logN);
            cfg.pssShuffleLength = Math.min(cfg.pssViewSize, derivedShuffleLength);
        } else {
            cfg.pssShuffleLength = Math.min(cfg.pssShuffleLength, cfg.pssViewSize);
        }

        if (cfg.replFanout == null || cfg.replFanout <= 0) {
            cfg.replFanout = Math.min(maxPeers, 2);
        } else {
            cfg.replFanout = Math.min(cfg.replFanout, maxPeers);
        }

        if (cfg.replTtl == null || cfg.replTtl <= 0) {
            cfg.replTtl = logN;
        }
    }

    private static int ceilLog2(int value) {
        if (value <= 1) return 1;

        int remaining = value - 1;
        int log = 0;
        while (remaining > 0) {
            remaining >>= 1;
            log++;
        }
        return log;
    }

    private static Config cloneConfig(Config src) {
        Config clone = new Config();
        clone.path = src.path;
        clone.separator = src.separator;
        clone.idColumn = src.idColumn;

        clone.mode = src.mode;
        clone.nodes = src.nodes;
        clone.pssViewSize = src.pssViewSize;
        clone.pssShuffleLength = src.pssShuffleLength;
        clone.pssCycleEveryItems = src.pssCycleEveryItems;

        clone.replFanout = src.replFanout;
        clone.replBatchSize = src.replBatchSize;
        clone.replCycleEveryItems = src.replCycleEveryItems;
        clone.replTtl = src.replTtl;
        clone.replSeenCacheSize = src.replSeenCacheSize;

        clone.dataKeepRatios = src.dataKeepRatios;
        clone.keepRatio = src.keepRatio;
        clone.maxStoredItems = src.maxStoredItems;
        clone.maxRepresentatives = src.maxRepresentatives;
        clone.refreshUtilitySpan = src.refreshUtilitySpan;
        clone.nonRepSampleFactor = src.nonRepSampleFactor;

        clone.padmeBinBalanceGamma = src.padmeBinBalanceGamma;
        clone.padmeBinBalanceMax = src.padmeBinBalanceMax;
        clone.padmeBinBalanceMin = src.padmeBinBalanceMin;

        clone.reportEvery = src.reportEvery;
        clone.ignoreColumns = src.ignoreColumns;
        clone.vectorTransform = src.vectorTransform;

        return clone;
    }

    private static String normalizeMode(String mode) {
        if (mode == null || mode.isBlank()) return "padme";

        String normalized = mode.trim().toLowerCase();
        return switch (normalized) {
            case "graphcut", "graph_cut" -> "graph_cut";
            case "maxdiversity", "max_diversity" -> "max_diversity";
            case "kcenter", "k_center" -> "k_center";
            case "baseline", "random", "padme" -> normalized;
            default -> normalized;
        };
    }

    private static boolean usesRepresentatives(String mode) {
        return mode.equals("padme")
                || mode.equals("graph_cut")
                || mode.equals("max_diversity")
                || mode.equals("k_center");
    }

    private static final class StorageSummary {
        private final int minStored;
        private final double meanStored;
        private final int maxStored;
        private final int underfilledNodes;

        private StorageSummary(int minStored, double meanStored, int maxStored, int underfilledNodes) {
            this.minStored = minStored;
            this.meanStored = meanStored;
            this.maxStored = maxStored;
            this.underfilledNodes = underfilledNodes;
        }
    }
}
