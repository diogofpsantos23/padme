package padme.dist;

import padme.config.Config;
import padme.config.ConfigLoader;
import padme.data.CsvRowReader;
import padme.dist.msg.AckMsg;
import padme.dist.msg.CtrlMsg;
import padme.dist.msg.RecordMsg;
import padme.dist.msg.ReplicateMsg;
import padme.dist.msg.RowMsg;
import padme.dist.net.FramedJson;
import padme.feature.NumericVectorMapper;
import padme.feature.VectorMapper;
import padme.math.L2Distance;
import padme.metrics.Metrics;
import padme.model.DataItem;
import padme.model.ItemMetadata;
import padme.model.Record;
import padme.node.Node;
import padme.retention.BaselineFullRetentionPolicy;
import padme.retention.PadmeRetentionPolicy;
import padme.retention.RepresentativeSet;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.InMemoryKvStore;
import padme.store.KvStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NodeProcess {
    private NodeProcess() {}

    public static void run(String[] args) {
        Map<String, String> m = Args.parse(args, 1);

        Path cfgPath = Path.of(Args.get(m, "config", "config.json"));
        Config cfg = cfgPath.toString().equals("config.json") ? ConfigLoader.loadDefault() : ConfigLoader.loadFromFile(cfgPath);

        int nodeId = Args.getInt(m, "nodeId", -1);
        if (nodeId < 0) throw new IllegalArgumentException("--nodeId is required");

        String input = Args.get(m, "input", null);
        if (input == null || input.isBlank()) throw new IllegalArgumentException("--input is required");

        int port = Args.getInt(m, "port", cfg.distBasePort + nodeId);

        String peersRaw = Args.get(m, "peers", "");
        List<Peer> peers = parsePeers(peersRaw);

        String collectorHost = Args.get(m, "collectorHost", "127.0.0.1");
        int collectorPort = Args.getInt(m, "collectorPort", cfg.distCollectorPort);

        Metrics metrics = new Metrics();
        RetentionPolicy policy = createPolicy(cfg, metrics);
        Node node = createNode(nodeId, policy);

        AtomicBoolean running = new AtomicBoolean(true);
        ServerSocket server;
        try {
            server = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to bind node server on port " + port, e);
        }

        Thread serverThread = new Thread(() -> runServerLoop(server, node, metrics, running), "node-server-" + nodeId);
        serverThread.setDaemon(true);
        serverThread.start();

        try (Socket ctrlSock = new Socket(collectorHost, collectorPort)) {
            DataInputStream cin = new DataInputStream(ctrlSock.getInputStream());
            DataOutputStream cout = new DataOutputStream(ctrlSock.getOutputStream());

            FramedJson.write(cout, CtrlMsg.hello(nodeId, cfg.mode));

            String[] header;
            try (CsvRowReader rdr = new CsvRowReader(input, cfg.separator, true)) {
                header = rdr.header();
                VectorMapper mapper = NumericVectorMapper.fromHeader(header, cfg.idColumn, cfg.ignoreColumns);
                ingestAll(cfg, rdr, mapper, node, metrics);
            }

            FramedJson.write(cout, CtrlMsg.ingestDone(nodeId));

            Object lock = new Object();
            int[] nextRound = new int[] { -1 };
            boolean[] stop = new boolean[] { false };
            boolean[] collect = new boolean[] { false };
            int[] graceHolder = new int[] { cfg.distGraceMs };

            Thread ctrlThread = new Thread(() -> {
                try {
                    while (true) {
                        CtrlMsg msg = FramedJson.read(cin, CtrlMsg.class);
                        if (msg == null) break;
                        if (msg.type == null) continue;
                        switch (msg.type) {
                            case "START_ROUND" -> {
                                synchronized (lock) {
                                    nextRound[0] = (msg.round == null) ? 0 : msg.round;
                                    lock.notifyAll();
                                }
                            }
                            case "STOP" -> {
                                graceHolder[0] = (msg.graceMs == null) ? cfg.distGraceMs : msg.graceMs;
                                synchronized (lock) {
                                    stop[0] = true;
                                    lock.notifyAll();
                                }
                            }
                            case "COLLECT" -> {
                                synchronized (lock) {
                                    collect[0] = true;
                                    lock.notifyAll();
                                }
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }, "node-ctrl-" + nodeId);
            ctrlThread.setDaemon(true);
            ctrlThread.start();

            long seed = (cfg.distSeed == null ? 1337L : cfg.distSeed) + (nodeId * 31L);
            Random rnd = new Random(seed);

            int maxRounds = cfg.distMaxRounds;
            int round = 0;

            while (round < maxRounds) {
                synchronized (lock) {
                    while (!stop[0] && nextRound[0] < 0) {
                        lock.wait();
                    }
                    if (stop[0]) break;
                    round = nextRound[0];
                    nextRound[0] = -1;
                }

                int qBefore = node.replicationQueueSize();
                runOneRound(cfg, nodeId, node, metrics, peers, rnd, round);
                FramedJson.write(cout, CtrlMsg.roundDone(nodeId, round, qBefore));
            }

            synchronized (lock) {
                while (!stop[0]) lock.wait();
            }
            int graceMs = graceHolder[0];
            if (graceMs > 0) {
                try { Thread.sleep(graceMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

            synchronized (lock) {
                while (!collect[0]) lock.wait();
            }
            streamResults(nodeId, node, cout);
            running.set(false);
            try { server.close(); } catch (Exception ignored) {}

        } catch (Exception e) {
            running.set(false);
            try { server.close(); } catch (Exception ignored) {}
            throw new RuntimeException("Node process failed", e);
        }
    }

    private static void ingestAll(Config cfg, CsvRowReader rdr, VectorMapper mapper, Node node, Metrics metrics) throws IOException {
        String[] row;
        long rowIdx = 0;
        while ((row = rdr.nextRow()) != null) {
            long key = computeKey(cfg, row, rowIdx);
            DataItem item = new DataItem(row.clone());
            float[] vector = mapper.map(row, cfg.idColumn);
            RetentionDecision d = node.onLocalItem(key, item, vector);
            metrics.seen++;
            metrics.record(d);
            rowIdx++;
        }
    }

    private static void runOneRound(Config cfg, int nodeId, Node node, Metrics metrics, List<Peer> peers, Random rnd, int round) {
        if (peers.isEmpty()) return;
        int fanout = Math.max(1, Math.min(cfg.replFanout, peers.size()));
        int batchSize = Math.max(1, cfg.replBatchSize);

        List<Peer> fanoutPeers = new ArrayList<>(peers);
        Collections.shuffle(fanoutPeers, new Random(rnd.nextLong() ^ ((long) round * 0x9E3779B97F4A7C15L)));
        if (fanoutPeers.size() > fanout) fanoutPeers = fanoutPeers.subList(0, fanout);

        while (true) {
            List<Record> batch = node.drainReplicationBatch(batchSize);
            if (batch.isEmpty()) break;

            List<RecordMsg> rms = new ArrayList<>(batch.size());
            for (Record r : batch) rms.add(toMsg(r));

            ReplicateMsg msg = new ReplicateMsg(nodeId, rms);
            for (Peer p : fanoutPeers) {
                if (p.id == nodeId) continue;
                try {
                    sendReplicate(p, msg);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static void sendReplicate(Peer peer, ReplicateMsg msg) throws IOException {
        try (Socket s = new Socket(peer.host, peer.port)) {
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());
            FramedJson.write(out, msg);
            FramedJson.read(in, AckMsg.class);
        }
    }

    private static void runServerLoop(ServerSocket server, Node node, Metrics metrics, AtomicBoolean running) {
        while (running.get()) {
            try (Socket s = server.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());

                ReplicateMsg msg = FramedJson.read(in, ReplicateMsg.class);
                if (msg != null && msg.records != null) {
                    for (RecordMsg rm : msg.records) {
                        Record r = fromMsg(rm);
                        RetentionDecision d = node.onRemoteRecord(r);
                        metrics.seen++;
                        metrics.record(d);
                    }
                }
                FramedJson.write(out, new AckMsg(node.id));
            } catch (IOException e) {
                if (running.get()) {
                    try { Thread.sleep(5); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static void streamResults(int nodeId, Node node, DataOutputStream out) throws IOException {
        int chunkSize = 1024;

        List<Record> snap = node.snapshotRecords();
        System.out.println("Node " + nodeId + " COLLECT snapshotRecords=" + snap.size());

        List<RowMsg> chunk = new ArrayList<>(chunkSize);
        for (Record r : snap) {
            chunk.add(new RowMsg(r.key, r.item.fields));
            if (chunk.size() >= chunkSize) {
                FramedJson.write(out, CtrlMsg.resultsChunk(nodeId, chunk));
                chunk = new ArrayList<>(chunkSize);
            }
        }
        if (!chunk.isEmpty()) {
            FramedJson.write(out, CtrlMsg.resultsChunk(nodeId, chunk));
        }
        FramedJson.write(out, CtrlMsg.resultsEnd(nodeId));
    }

    private static RecordMsg toMsg(Record r) {
        RecordMsg m = new RecordMsg();
        m.key = r.key;
        m.fields = r.item.fields;
        m.version = r.meta.version;
        m.utility = r.meta.utility;
        m.vector = r.meta.vector;
        return m;
    }

    private static Record fromMsg(RecordMsg m) {
        if (m == null) return null;
        DataItem item = new DataItem(m.fields);
        ItemMetadata meta = new ItemMetadata(m.version, m.vector, m.utility);
        return new Record(m.key, item, meta);
    }

    private static List<Peer> parsePeers(String peersRaw) {
        String t = (peersRaw == null) ? "" : peersRaw.trim();
        if (t.isEmpty()) return List.of();
        String[] parts = t.split(",");
        List<Peer> peers = new ArrayList<>(parts.length);
        for (String p : parts) {
            String s = (p == null) ? "" : p.trim();
            if (s.isEmpty()) continue;
            peers.add(Peer.parse(s));
        }
        return peers;
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
}