package padme.node;

import padme.model.DataItem;
import padme.model.ItemMetadata;
import padme.model.Record;
import padme.retention.RetentionDecision;
import padme.retention.RetentionPolicy;
import padme.store.KvStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public final class Node {
    public final int id;

    private final RetentionPolicy retention;
    private final KvStore kv;
    private final int replTtl;

    private int dim = -1;
    private long versionCounter = 0;

    private final Deque<QueuedRecord> replQueue = new ArrayDeque<>();
    private final int replQueueMax = 100_000;

    public Node(int id, RetentionPolicy retention, KvStore kv, int replTtl) {
        this.id = id;
        this.retention = retention;
        this.kv = kv;
        this.replTtl = Math.max(1, replTtl);
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
        enqueueForReplication(rec, replTtl);
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
        enqueueForReplication(rec, replTtl);

        return d;
    }

    public List<Record> drainReplicationBatch(int max) {
        int n = Math.max(0, max);
        List<Record> out = new ArrayList<>(Math.min(n, replQueue.size()));
        while (n-- > 0) {
            QueuedRecord queued = replQueue.pollFirst();
            if (queued == null) break;
            out.add(queued.record);

            int nextTtl = queued.ttlRemaining - 1;
            if (nextTtl > 0) {
                enqueueForReplication(queued.record, nextTtl);
            }
        }
        return out;
    }

    public int replicationQueueSize() {
        return replQueue.size();
    }

    private void enqueueForReplication(Record rec, int ttlRemaining) {
        if (rec == null || ttlRemaining <= 0) return;
        if (replQueue.size() >= replQueueMax) {
            replQueue.pollFirst();
        }
        replQueue.addLast(new QueuedRecord(rec, ttlRemaining));
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

    private static final class QueuedRecord {
        private final Record record;
        private final int ttlRemaining;

        private QueuedRecord(Record record, int ttlRemaining) {
            this.record = record;
            this.ttlRemaining = ttlRemaining;
        }
    }
}