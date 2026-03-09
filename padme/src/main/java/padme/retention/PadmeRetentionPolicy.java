package padme.retention;

import padme.metrics.Metrics;
import padme.store.HeapEntry;
import padme.store.HeapItemStore;
import padme.store.ItemStore;

public final class PadmeRetentionPolicy implements RetentionPolicy {
    private final ItemStore store;
    private final RepresentativeSet reps;
    private final Metrics metrics;

    private final int refreshEveryItems;
    private long admittedSinceStart = 0;

    private double totalUtility = 0.0;

    public PadmeRetentionPolicy(int maxStoredItems, RepresentativeSet reps, int refreshEveryItems, Metrics metrics) {
        this.reps = reps;
        this.refreshEveryItems = refreshEveryItems;
        this.metrics = metrics;
        this.store = new HeapItemStore(Math.max(0, maxStoredItems));
    }

    @Override
    public RetentionDecision onItem(long key, float[] vector) {
        if (store.capacity() == 0) {
            if (metrics != null) {
                metrics.winRecordSeen(0.0);
                metrics.winRecordDropped(0.0);
            }
            return RetentionDecision.dropped();
        }

        RepresentativeSet.UtilityScore score;
        double u;

        if (reps.isEmpty()) {
            score = new RepresentativeSet.UtilityScore(-1L, Double.POSITIVE_INFINITY);
            u = Double.POSITIVE_INFINITY;
        } else {
            score = reps.score(key, vector);
            u = score.utility;

            if (!Double.isFinite(u)) {
                if (metrics != null) {
                    metrics.winRecordSeen(u);
                    metrics.winRecordDropped(u);
                }
                return RetentionDecision.dropped();
            }
        }

        if (metrics != null) metrics.winRecordSeen(u);

        HeapEntry incoming = new HeapEntry(key, vector, u, score.nearestRepKey, false);

        if (store.size() < store.capacity()) {
            return admitWithFreeCapacity(incoming);
        }

        return admitAtCapacity(incoming);
    }

    private RetentionDecision admitWithFreeCapacity(HeapEntry incoming) {
        boolean becameRep = false;
        RepresentativeSet.Change repChange = RepresentativeSet.Change.none();

        if (!reps.isFull()) {
            repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.utility);
            becameRep = repChange.changed && repChange.addedRepKey == incoming.key;
        } else if (incoming.utility > reps.minUtility()) {
            repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.utility);
            becameRep = repChange.changed && repChange.addedRepKey == incoming.key;
        }

        if (becameRep) {
            incoming.representative = true;
            store.addRepresentative(incoming);
            if (repChange.removedRepKey >= 0L) {
                store.markNonRepresentative(repChange.removedRepKey);
            }
        } else {
            store.addNonRepresentative(incoming);
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey);
        if (repChange.changed) maybeRefreshReps();

        if (metrics != null) {
            metrics.winRecordAdmitted(incoming.utility);
            if (repChange.changed) metrics.winRecordRepReplaced();
        }

        recomputeTotalUtility();
        return RetentionDecision.admitted(incoming);
    }

    private RetentionDecision admitAtCapacity(HeapEntry incoming) {
        double worstNonRepU = store.minNonRepresentativeUtility();
        boolean canEvictNonRep = hasEvictableNonRepresentative();
        boolean shouldBecomeRep = !reps.isFull() || incoming.utility > reps.minUtility();

        if (!shouldBecomeRep) {
            if (!canEvictNonRep || incoming.utility <= worstNonRepU) {
                if (metrics != null) metrics.winRecordDropped(incoming.utility);
                return RetentionDecision.dropped();
            }

            HeapEntry out = store.evictWorstNonRepresentative();
            store.addNonRepresentative(incoming);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            recomputeTotalUtility();
            return RetentionDecision.evictedAndAdmitted(out, incoming);
        }

        if (!canEvictNonRep) {
            if (metrics != null) metrics.winRecordDropped(incoming.utility);
            return RetentionDecision.dropped();
        }

        RepresentativeSet.Change repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.utility);
        boolean becameRep = repChange.changed && repChange.addedRepKey == incoming.key;

        if (!becameRep) {
            if (incoming.utility <= worstNonRepU) {
                if (metrics != null) metrics.winRecordDropped(incoming.utility);
                return RetentionDecision.dropped();
            }

            HeapEntry out = store.evictWorstNonRepresentative();
            store.addNonRepresentative(incoming);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            recomputeTotalUtility();
            return RetentionDecision.evictedAndAdmitted(out, incoming);
        }

        incoming.representative = true;
        store.addRepresentative(incoming);

        long demotedRepKey = repChange.removedRepKey;
        if (demotedRepKey >= 0L) {
            store.markNonRepresentative(demotedRepKey);
        }

        HeapEntry out = store.evictWorstNonRepresentativeExcept(demotedRepKey);
        if (out == null) {
            out = store.evictWorstNonRepresentative();
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey);
        maybeRefreshReps();

        if (metrics != null) {
            if (out != null) metrics.winRecordEvicted();
            metrics.winRecordAdmitted(incoming.utility);
            metrics.winRecordRepReplaced();
        }

        recomputeTotalUtility();

        if (out == null) {
            return RetentionDecision.admitted(incoming);
        }
        return RetentionDecision.evictedAndAdmitted(out, incoming);
    }

    private boolean hasEvictableNonRepresentative() {
        for (HeapEntry e : store.nonRepresentativeEntries()) {
            return true;
        }
        return false;
    }

    private void applyRepChanges(long removedRepKey, long addedRepKey) {
        if (store.size() == 0) return;
        if (removedRepKey < 0L && addedRepKey < 0L) return;

        boolean touched = false;

        for (HeapEntry e : store.entries()) {
            if (e.representative) continue;

            boolean mustRecompute = e.key == addedRepKey;
            if (removedRepKey >= 0L && e.nearestRepKey == removedRepKey) {
                mustRecompute = true;
            }

            if (mustRecompute) {
                refreshEntryFromScratch(e);
                touched = true;
            }
        }

        if (addedRepKey >= 0L) {
            for (HeapEntry e : store.nonRepresentativeEntries()) {
                if (e.key == addedRepKey) continue;

                double d = reps.distanceToRep(addedRepKey, e.vector);
                if (!Double.isFinite(d)) continue;

                if (!Double.isFinite(e.utility) || d < e.utility) {
                    e.utility = d;
                    e.nearestRepKey = addedRepKey;
                    touched = true;
                }
            }
        }

        if (touched) {
            store.rebuildHeap();
        }
    }

    private void refreshEntryFromScratch(HeapEntry e) {
        RepresentativeSet.UtilityScore s = reps.score(e.key, e.vector);
        e.utility = s.utility;
        e.nearestRepKey = s.nearestRepKey;
    }

    private void recomputeTotalUtility() {
        double sum = 0.0;
        for (HeapEntry e : store.entries()) {
            if (Double.isFinite(e.utility)) {
                sum += e.utility;
            }
        }
        totalUtility = sum;
    }

    private void maybeRefreshReps() {
        if (refreshEveryItems > 0 && (admittedSinceStart % refreshEveryItems == 0)) {
            reps.refreshUtilities();
        }
    }

    @Override
    public int storedCount() { return store.size(); }

    @Override
    public double totalUtility() { return totalUtility; }

    public int representativeCount() { return reps.size(); }

    public double minUtilityStored() {
        return store.size() == 0 ? Double.NaN : store.minNonRepresentativeUtility();
    }

    public double repsMinUtility() { return reps.minUtility(); }
    public double repsMeanUtility() { return reps.meanUtility(); }
}