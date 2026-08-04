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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Node {
    public final int id;

    private final RetentionPolicy retention;
    private final KvStore kv;
    private final int replTtl;
    private final Map<Long, Long> seenVersions;

    private int dim = -1;
    private long versionCounter = 0;

    private final Deque<QueuedRecord> replQueue = new ArrayDeque<>();
    private final int replQueueMax = 100_000;

    public Node(int id, RetentionPolicy retention, KvStore kv, int replTtl, int replSeenCacheSize) {
        this.id = id;
        this.retention = retention;
        this.kv = kv;
        this.replTtl = Math.max(1, replTtl);

        int seenCapacity = Math.max(1, replSeenCacheSize);
        this.seenVersions = new LinkedHashMap<>(Math.min(seenCapacity, 16_384), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Long> eldest) {
                return size() > seenCapacity;
            }
        };
    }

    public RetentionDecision onLocalItem(long key, DataItem item, float[] vector) {
        if (dim < 0) dim = vector.length;

        RetentionDecision decision = retention.onItem(key, vector);
        if (decision.kind == RetentionDecision.Kind.DROPPED) {
            return decision;
        }

        if (decision.evicted != null) {
            kv.evict(decision.evicted.key);
        }

        long version = ++versionCounter;
        version = (((long) id) << 48) | (version & 0x0000FFFFFFFFFFFFL);

        ItemMetadata metadata = new ItemMetadata(version, vector, decision.admitted.utility);
        Record record = new Record(key, item, metadata);
        kv.put(key, record);
        rememberVersion(key, version);
        enqueueForReplication(record, replTtl);
        return decision;
    }

    public RetentionDecision onRemoteRecord(Record incoming, int ttlRemaining) {
        if (incoming == null || incoming.meta == null) return RetentionDecision.dropped();

        float[] vector = incoming.meta.vector;
        if (vector == null) return RetentionDecision.dropped();
        if (dim < 0) dim = vector.length;

        Record current = kv.get(incoming.key);
        if (current != null && current.meta != null && current.meta.version >= incoming.meta.version) {
            return RetentionDecision.droppedDuplicate();
        }

        if (!rememberVersionIfNewer(incoming.key, incoming.meta.version)) {
            return RetentionDecision.droppedDuplicate();
        }

        RetentionDecision decision = retention.onItem(incoming.key, vector);
        Record recordToForward = incoming;

        if (decision.kind != RetentionDecision.Kind.DROPPED) {
            if (decision.evicted != null) {
                kv.evict(decision.evicted.key);
            }

            ItemMetadata metadata = new ItemMetadata(incoming.meta.version, vector, decision.admitted.utility);
            Record storedRecord = new Record(incoming.key, incoming.item, metadata);
            kv.put(incoming.key, storedRecord);
            recordToForward = storedRecord;
        }

        if (ttlRemaining > 1) {
            enqueueForReplication(recordToForward, ttlRemaining - 1);
        }

        return decision;
    }

    public List<QueuedRecord> drainReplicationBatch(int max) {
        int remaining = Math.max(0, max);
        List<QueuedRecord> out = new ArrayList<>(Math.min(remaining, replQueue.size()));
        while (remaining-- > 0) {
            QueuedRecord queued = replQueue.pollFirst();
            if (queued == null) break;
            out.add(queued);
        }
        return out;
    }

    public int replicationQueueSize() {
        return replQueue.size();
    }

    private void enqueueForReplication(Record record, int ttlRemaining) {
        if (record == null || ttlRemaining <= 0) return;

        if (replQueue.size() >= replQueueMax) {
            replQueue.pollFirst();
        }
        replQueue.addLast(new QueuedRecord(record, ttlRemaining));
    }

    private void rememberVersion(long key, long version) {
        Long current = seenVersions.get(key);
        if (current == null || version > current) {
            seenVersions.put(key, version);
        }
    }

    private boolean rememberVersionIfNewer(long key, long version) {
        Long current = seenVersions.get(key);
        if (current != null && current >= version) {
            return false;
        }
        seenVersions.put(key, version);
        return true;
    }

    public List<Record> snapshotRecords() {
        List<Record> out = new ArrayList<>(storedCount());
        for (Record record : kv.values()) out.add(record);
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

    public static final class QueuedRecord {
        public final Record record;
        public final int ttlRemaining;

        public QueuedRecord(Record record, int ttlRemaining) {
            this.record = record;
            this.ttlRemaining = ttlRemaining;
        }
    }
}
