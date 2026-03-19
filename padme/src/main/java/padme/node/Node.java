package padme.node;

import padme.model.DataItem;
import padme.model.ItemMetadata;
import padme.model.Record;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.KvStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Random;

public final class Node {
    public final int id;

    private final String mode;
    private final RetentionPolicy retention;
    private final KvStore kv;

    private final int forwardWindowSize;
    private final double forwardRatio;
    private final List<Record> localForwardBuffer = new ArrayList<>();
    private final Random rng;

    private int dim = -1;
    private long versionCounter = 0;

    private final Deque<Record> replQueue = new ArrayDeque<>();
    private final int replQueueMax = 100_000;

    public Node(int id, String mode, RetentionPolicy retention, KvStore kv, int forwardWindowSize, double forwardRatio) {
        this.id = id;
        this.mode = mode;
        this.retention = retention;
        this.kv = kv;
        this.forwardWindowSize = forwardWindowSize;
        this.forwardRatio = forwardRatio;
        this.rng = new Random(1337L + id);
    }

    public RetentionDecision onLocalItem(long key, DataItem item, float[] vector) {
        if (dim < 0) dim = vector.length;

        RetentionDecision d = retention.onItem(key, vector);

        if (d.kind == RetentionDecision.Kind.DROPPED) {
            return d;
        }

        if (d.evicted != null) {
            kv.evict(d.evicted.key);
        }

        long v = ++versionCounter;
        v = (((long) id) << 48) | (v & 0x0000FFFFFFFFFFFFL);

        ItemMetadata meta = new ItemMetadata(v, vector, d.admitted.utility);
        Record rec = new Record(key, item, meta);
        kv.put(key, rec);

        bufferLocalForForwarding(rec);
        return d;
    }

    public RetentionDecision onRemoteRecord(Record incoming) {
        if (incoming == null || incoming.meta == null) return RetentionDecision.dropped();

        float[] vector = incoming.meta.vector;
        if (vector == null) return RetentionDecision.dropped();
        if (dim < 0) dim = vector.length;

        Record current = kv.get(incoming.key);
        if (current != null && current.meta != null && current.meta.version >= incoming.meta.version) {
            return RetentionDecision.droppedDuplicate();
        }

        RetentionDecision d = retention.onItem(incoming.key, vector);
        if (d.kind == RetentionDecision.Kind.DROPPED) {
            return d;
        }

        if (d.evicted != null) {
            kv.evict(d.evicted.key);
        }

        ItemMetadata meta = new ItemMetadata(incoming.meta.version, vector, d.admitted.utility);
        Record rec = new Record(incoming.key, incoming.item, meta);
        kv.put(incoming.key, rec);

        return d;
    }

    public List<Record> drainReplicationBatch(int max) {
        int n = Math.max(0, max);
        List<Record> out = new ArrayList<>(Math.min(n, replQueue.size()));
        while (n-- > 0) {
            Record r = replQueue.pollFirst();
            if (r == null) break;
            out.add(r);
        }
        return out;
    }

    public int replicationQueueSize() {
        return replQueue.size();
    }

    public void flushPendingForwardWindow() {
        flushForwardWindow();
    }

    private void bufferLocalForForwarding(Record rec) {
        if (rec == null) return;

        localForwardBuffer.add(rec);

        if (localForwardBuffer.size() >= forwardWindowSize) {
            flushForwardWindow();
        }
    }

    private void flushForwardWindow() {
        if (localForwardBuffer.isEmpty()) return;

        if (mode.equalsIgnoreCase("baseline")) {
            for (Record r : localForwardBuffer) {
                enqueueForReplication(r);
            }
            localForwardBuffer.clear();
            return;
        }

        int n = localForwardBuffer.size();
        int k = (int) Math.round(n * forwardRatio);

        if (k <= 0) {
            localForwardBuffer.clear();
            return;
        }

        if (k >= n) {
            for (Record r : localForwardBuffer) {
                enqueueForReplication(r);
            }
            localForwardBuffer.clear();
            return;
        }

        if (mode.equalsIgnoreCase("random")) {
            List<Record> shuffled = new ArrayList<>(localForwardBuffer);
            Collections.shuffle(shuffled, rng);

            for (int i = 0; i < k; i++) {
                enqueueForReplication(shuffled.get(i));
            }

            localForwardBuffer.clear();
            return;
        }

        if (mode.equalsIgnoreCase("padme")) {
            List<Record> ranked = new ArrayList<>(localForwardBuffer);
            ranked.sort((a, b) -> Double.compare(safeUtility(b), safeUtility(a)));

            for (int i = 0; i < k; i++) {
                enqueueForReplication(ranked.get(i));
            }

            localForwardBuffer.clear();
            return;
        }

        for (Record r : localForwardBuffer) {
            enqueueForReplication(r);
        }
        localForwardBuffer.clear();
    }

    private double safeUtility(Record r) {
        if (r == null || r.meta == null) return Double.NEGATIVE_INFINITY;
        return r.meta.utility;
    }

    private void enqueueForReplication(Record rec) {
        if (rec == null) return;
        if (replQueue.size() >= replQueueMax) {
            replQueue.pollFirst();
        }
        replQueue.addLast(rec);
    }

    public List<Record> snapshotRecords() {
        List<Record> out = new ArrayList<>(storedCount());
        for (Record r : kv.values()) out.add(r);
        return out;
    }

    public int storedCount() {
        return retention.storedCount();
    }

    public long storedBytes() {
        int storedCount = storedCount();
        if (storedCount <= 0 || dim <= 0) return 0L;
        long bytesPerItem = 8L + 8L + 8L + (4L * dim) + (4L * dim);
        return bytesPerItem * storedCount;
    }

    public double totalUtility() {
        return retention.totalUtility();
    }
}