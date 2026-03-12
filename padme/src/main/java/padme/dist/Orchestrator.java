package padme.dist;

import padme.config.Config;
import padme.config.ConfigLoader;
import padme.data.CsvRowReader;
import padme.dist.msg.CtrlMsg;
import padme.dist.msg.RowMsg;
import padme.dist.net.FramedJson;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class Orchestrator {
    private Orchestrator() {}

    public static void run(String[] args) {
        Map<String, String> m = Args.parse(args, 1);
        String cfgArg = Args.get(m, "config", "config.json");
        Path cfgPath = Path.of(cfgArg);
        Config cfg = cfgArg.equals("config.json") ? ConfigLoader.loadDefault() : ConfigLoader.loadFromFile(cfgPath);

        if (cfg.nodes <= 1) {
            throw new IllegalArgumentException("config.nodes must be > 1 for orchestrator");
        }

        int n = cfg.nodes;

        String tmpDirRaw = null;
        Path tmpDir = tmpDirRaw.isEmpty()
                ? Path.of(System.getProperty("java.io.tmpdir"), "padme-partitions-" + Instant.now().toEpochMilli())
                : Path.of(tmpDirRaw);

        Path inputsDir = tmpDir.resolve("inputs");
        Path runCfgPath = tmpDir.resolve("config.json");

        ensureGlobalIdConfig(cfg);
        ConfigLoader.writeToFile(cfg, runCfgPath);

        System.out.println("Loaded config: " + cfg);
        System.out.println("Working dir: " + tmpDir.toAbsolutePath());

        String[] header;
        try {
            header = partitionSingleInputIntoNodeInputs(cfg, inputsDir);
        } catch (Exception e) {
            throw new RuntimeException("Partitioning failed", e);
        }

        Map<Integer, CtrlConn> conns = new HashMap<>();
        List<Process> procs = new ArrayList<>();

        Thread shutdownHook = new Thread(() -> killAll(procs), "padme-kill-nodes");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            try (ServerSocket ctrlServer = new ServerSocket(cfg.distCollectorPort)) {
                procs.addAll(spawnNodes(cfg, runCfgPath, inputsDir));

                while (conns.size() < n) {
                    Socket s = ctrlServer.accept();

                    s.setSoTimeout(5000);
                    CtrlConn c = new CtrlConn(s);

                    CtrlMsg hello;
                    try {
                        hello = FramedJson.read(c.in, CtrlMsg.class);
                    } catch (SocketTimeoutException te) {
                        c.close();
                        continue;
                    }

                    if (hello == null || hello.nodeId == null) {
                        c.close();
                        continue;
                    }

                    c.sock.setSoTimeout(100);

                    conns.put(hello.nodeId, c);
                    System.out.println("Connected node " + hello.nodeId + " mode=" + hello.mode);
                }

                waitIngestDone(conns, n);

                int round = 0;
                int maxRounds = cfg.distMaxRounds;

                int idleRounds = 0;
                int K = 5;

                while (round < maxRounds) {
                    broadcast(conns, CtrlMsg.startRound(round));
                    int nonEmpty = waitRoundDone(conns, n, round);

                    if (nonEmpty == 0) {
                        idleRounds++;
                    } else {
                        idleRounds = 0;
                    }

                    if (idleRounds >= K) {
                        break;
                    }

                    round++;
                }

                broadcast(conns, CtrlMsg.stop(cfg.distGraceMs));

                Path out = resolveOutPath(cfg);
                collectMergedNoDedup(conns, n, header, out);
            }

            long waitMs = (cfg.distGraceMs <= 0) ? 3000L : (cfg.distGraceMs + 3000L);

            for (Process p : procs) {
                if (p == null) continue;
                try {
                    if (!p.waitFor(waitMs, TimeUnit.MILLISECONDS)) {
                        p.destroy();
                        if (!p.waitFor(1000, TimeUnit.MILLISECONDS)) {
                            p.destroyForcibly();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (Exception e) {
            killAll(procs);
            throw new RuntimeException("Orchestrator failed", e);
        } finally {
            for (CtrlConn c : conns.values()) {
                try { c.close(); } catch (Exception ignored) {}
            }

            killAll(procs);

            try { Runtime.getRuntime().removeShutdownHook(shutdownHook); }
            catch (IllegalStateException ignored) {
            }
        }
    }

    private static void killAll(List<Process> procs) {
        for (Process p : procs) {
            if (p == null) continue;
            try {
                ProcessHandle h = p.toHandle();
                h.descendants().forEach(ph -> {
                    try {
                        if (ph.isAlive()) ph.destroy();
                    } catch (Exception ignored) {}
                });

                if (h.isAlive()) h.destroy();

                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                if (h.isAlive()) h.destroyForcibly();
            } catch (Exception ignored) {}
        }
    }

    private static String[] partitionSingleInputIntoNodeInputs(Config cfg, Path inputsDir) throws IOException {
        Files.createDirectories(inputsDir);

        int n = cfg.nodes;
        long[] perNode = new long[n];

        List<BufferedWriter> writers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Path p = inputsDir.resolve("input_node_" + i + ".csv");
            writers.add(Files.newBufferedWriter(p, StandardCharsets.UTF_8));
        }

        String[] headerOut;

        try (CsvRowReader rdr = new CsvRowReader(cfg.path, cfg.separator, true)) {
            String[] headerIn = rdr.header();

            headerOut = new String[headerIn.length + 1];
            headerOut[0] = "__id";
            System.arraycopy(headerIn, 0, headerOut, 1, headerIn.length);

            String headerLine = joinCsv(headerOut);
            for (BufferedWriter w : writers) {
                w.write(headerLine);
                w.newLine();
            }

            String[] row;
            long rowIdx = 0;

            while ((row = rdr.nextRow()) != null) {
                long key = rowIdx;
                int owner = Hashing.bucket(key, n);

                String[] outRow = new String[row.length + 1];
                outRow[0] = Long.toString(key);
                System.arraycopy(row, 0, outRow, 1, row.length);

                BufferedWriter w = writers.get(owner);
                w.write(joinCsv(outRow));
                w.newLine();

                perNode[owner]++;
                rowIdx++;
            }
        } finally {
            for (BufferedWriter w : writers) {
                try { w.flush(); w.close(); } catch (Exception ignored) {}
            }
        }

        for (int i = 0; i < n; i++) {
            System.out.println("Partition insert -> node " + i + ": items=" + perNode[i]);
        }

        return headerOut;
    }

    private static List<Process> spawnNodes(Config cfg, Path runCfgPath, Path inputsDir) {
        int n = cfg.nodes;
        List<Process> procs = new ArrayList<>(n);

        String jarPath = resolveJarPath();
        if (jarPath == null) throw new IllegalStateException("Could not resolve jar path");

        String peers = buildPeers(cfg);

        for (int i = 0; i < n; i++) {
            String input = inputsDir.resolve("input_node_" + i + ".csv").toAbsolutePath().toString();
            int port = cfg.distBasePort + i;

            List<String> cmd = new ArrayList<>();
            cmd.add("java");
            cmd.add("-jar");
            cmd.add(jarPath);
            cmd.add("node");
            cmd.add("--config");
            cmd.add(runCfgPath.toAbsolutePath().toString());
            cmd.add("--nodeId");
            cmd.add(Integer.toString(i));
            cmd.add("--input");
            cmd.add(input);
            cmd.add("--port");
            cmd.add(Integer.toString(port));
            cmd.add("--peers");
            cmd.add(peers);
            cmd.add("--collectorHost");
            cmd.add("127.0.0.1");
            cmd.add("--collectorPort");
            cmd.add(Integer.toString(cfg.distCollectorPort));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.inheritIO();
            try {
                procs.add(pb.start());
            } catch (IOException e) {
                throw new RuntimeException("Failed to spawn node " + i, e);
            }
        }

        return procs;
    }

    private static void waitIngestDone(Map<Integer, CtrlConn> conns, int n) throws IOException {
        Set<Integer> done = new HashSet<>();

        while (done.size() < n) {
            boolean progressed = false;

            for (Map.Entry<Integer, CtrlConn> e : conns.entrySet()) {
                int nodeId = e.getKey();
                if (done.contains(nodeId)) continue;

                CtrlMsg msg = readCtrlNonBlocking(e.getValue());
                if (msg == null) continue;

                progressed = true;

                if ("INGEST_DONE".equals(msg.type)) {
                    done.add(nodeId);
                    System.out.println("Ingest done: node " + nodeId);
                }
            }

            if (!progressed) {
                try { Thread.sleep(2); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static int waitRoundDone(Map<Integer, CtrlConn> conns, int n, int round) throws IOException {
        Set<Integer> done = new HashSet<>();
        int nonEmpty = 0;

        while (done.size() < n) {
            boolean progressed = false;

            for (Map.Entry<Integer, CtrlConn> e : conns.entrySet()) {
                int nodeId = e.getKey();
                if (done.contains(nodeId)) continue;

                CtrlMsg msg = readCtrlNonBlocking(e.getValue());
                if (msg == null) continue;

                progressed = true;

                if ("ROUND_DONE".equals(msg.type) && msg.round != null && msg.round == round) {
                    done.add(nodeId);

                    int q = (msg.queueSize == null) ? 0 : msg.queueSize;
                    if (q > 0) nonEmpty++;
                }
            }

            if (!progressed) {
                try { Thread.sleep(2); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }

        //System.out.println("Round " + round + " done. nonEmptyQueues=" + nonEmpty);
        return nonEmpty;
    }

    private static void collectMergedNoDedup(Map<Integer, CtrlConn> conns, int n, String[] header, Path out) throws IOException {
        Files.createDirectories(out.getParent());

        long totalRows = 0;

        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            if (header != null && header.length > 0) {
                w.write(joinCsv(header));
                w.newLine();
            }

            for (int nodeId = 0; nodeId < n; nodeId++) {
                long rowsFromNode = 0;

                CtrlConn c = conns.get(nodeId);
                if (c == null) {
                    System.out.println("Collect: missing connection for node " + nodeId);
                    continue;
                }

                FramedJson.write(c.out, CtrlMsg.collect());

                while (true) {
                    CtrlMsg msg = readCtrlNonBlocking(c);
                    if (msg == null) {
                        try { Thread.sleep(2); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                        continue;
                    }

                    if ("RESULTS_CHUNK".equals(msg.type) && msg.rows != null) {
                        rowsFromNode += msg.rows.size();

                        for (RowMsg r : msg.rows) {
                            w.write(joinCsv(r.fields));
                            w.newLine();
                        }
                    } else if ("RESULTS_END".equals(msg.type)) {
                        break;
                    }
                }

                totalRows += rowsFromNode;
                System.out.println("Collect from node " + nodeId + ": rows=" + rowsFromNode);
            }
        }

        System.out.println("Collect total rows: " + totalRows);
        System.out.println("Wrote retained dataset: " + out.toAbsolutePath());
    }

    private static void broadcast(Map<Integer, CtrlConn> conns, CtrlMsg msg) throws IOException {
        for (CtrlConn c : conns.values()) {
            FramedJson.write(c.out, msg);
        }
    }

    private static CtrlMsg readCtrlNonBlocking(CtrlConn c) throws IOException {
        try {
            return FramedJson.read(c.in, CtrlMsg.class);
        } catch (SocketTimeoutException | EOFException te) {
            return null;
        }
    }

    private static Path resolveOutPath(Config cfg) {
        return cfg.mode.equalsIgnoreCase("baseline") ? Path.of("src/main/resources/data/baseline_latest.csv") : Path.of("src/main/resources/data/padme_latest.csv");
    }

    private static String resolveJarPath() {
        try {
            var uri = Orchestrator.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path p = Path.of(uri);
            return p.toAbsolutePath().toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static String buildPeers(Config cfg) {
        int n = cfg.nodes;
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(i + ":127.0.0.1:" + (cfg.distBasePort + i));
        }
        return String.join(",", out);
    }

    private static String joinCsv(String[] fields) {
        StringBuilder sb = new StringBuilder(fields.length * 8);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(escapeCsv(fields[i]));
        }
        return sb.toString();
    }

    private static String escapeCsv(String s) {
        if (s == null) return "";
        boolean needsQuotes = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuotes) return s;
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    private static void ensureGlobalIdConfig(Config cfg) {
        cfg.idColumn = 0;

        String[] cols = (cfg.ignoreColumns == null) ? new String[0] : cfg.ignoreColumns;

        boolean has = false;
        for (String c : cols) {
            if ("__id".equals(c)) { has = true; break; }
        }

        if (!has) {
            String[] next = Arrays.copyOf(cols, cols.length + 1);
            next[next.length - 1] = "__id";
            cfg.ignoreColumns = next;
        }
    }
}