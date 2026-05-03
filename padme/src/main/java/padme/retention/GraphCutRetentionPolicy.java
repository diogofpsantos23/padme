package padme.retention;

import padme.metrics.Metrics;
import padme.store.HeapEntry;
import padme.store.HeapItemStore;
import padme.store.ItemStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public final class GraphCutRetentionPolicy implements RetentionPolicy {
    private static final int BOOTSTRAP_REPRESENTATIVES = 1;
    private static final double STORE_ADMISSION_SLACK = 1.00;
    private static final double REP_PROMOTION_SLACK = 1.10;

    private final ItemStore store;
    private final RepresentativeSet reps;
    private final Metrics metrics;
    private final int refreshEveryItems;
    private final double lambda;
    private final int nonRepSampleFactor;

    private long admittedSinceStart = 0;
    private double totalUtility = 0.0;

    private final ArrayList<Long> sampledNonRepKeys = new ArrayList<>();

    public GraphCutRetentionPolicy(int maxStoredItems, RepresentativeSet reps, int refreshEveryItems, double lambda, int nonRepSampleFactor, Metrics metrics) {
        this.reps = reps;
        this.refreshEveryItems = refreshEveryItems;
        this.lambda = lambda;
        this.nonRepSampleFactor = Math.max(1, nonRepSampleFactor);
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

        RepresentativeSet.NeighborScore score;
        double storeU;
        double repU;

        if (reps.isEmpty()) {
            score = new RepresentativeSet.NeighborScore(-1L, Double.POSITIVE_INFINITY, -1L, Double.POSITIVE_INFINITY);
            storeU = Double.POSITIVE_INFINITY;
            repU = Double.POSITIVE_INFINITY;
        } else {
            score = reps.scoreWithSecond(key, vector);
            storeU = computeStoreUtility(key, vector);
            repU = RepresentativeSet.computeRepresentativeUtility(score.nearestUtility(), score.secondNearestUtility());

            if (!Double.isFinite(storeU)) {
                if (metrics != null) {
                    metrics.winRecordSeen(storeU);
                    metrics.winRecordDropped(storeU);
                }
                return RetentionDecision.dropped();
            }
        }

        if (metrics != null) {
            metrics.winRecordSeen(storeU);
        }

        HeapEntry incoming = new HeapEntry(
                key,
                vector,
                storeU,
                repU,
                score.nearestUtility(),
                score.nearestRepKey(),
                score.secondNearestUtility(),
                score.secondNearestRepKey(),
                false
        );

        if (store.size() < store.capacity()) {
            return admitWithFreeCapacity(incoming);
        }

        return admitAtCapacity(incoming);
    }

    private double computeStoreUtility(long itemKey, float[] vector) {
        if (reps.isEmpty()) {
            return Double.POSITIVE_INFINITY;
        }

        ensureSampleConsistency();

        double coverage = 0.0;
        for (HeapEntry rep : store.representativeEntries()) {
            if (rep.key == itemKey) continue;

            double d = reps.distanceToRep(rep.key, vector);
            if (!Double.isFinite(d)) continue;

            coverage += similarityFromDistance(d);
        }

        double redundancy = 0.0;
        for (long sampledKey : sampledNonRepKeys) {
            if (sampledKey == itemKey) continue;

            HeapEntry e = store.get(sampledKey);
            if (e == null || e.representative) continue;

            double d = distanceBetween(vector, e.vector);
            if (!Double.isFinite(d)) continue;

            redundancy += similarityFromDistance(d);
        }

        return (lambda * coverage) - (2.0 * redundancy);
    }

    private double similarityFromDistance(double d) {
        return 1.0 / (1.0 + Math.max(0.0, d));
    }

    private double distanceBetween(float[] a, float[] b) {
        return reps.distanceBetween(a, b);
    }

    private boolean shouldPromoteToRepresentative(HeapEntry incoming) {
        if (store.isRepresentative(incoming.key)) {
            return true;
        }

        if (!reps.isFull() && reps.size() < Math.min(BOOTSTRAP_REPRESENTATIVES, store.capacity())) {
            return true;
        }

        if (reps.isEmpty()) {
            return true;
        }

        return incoming.representativeUtility > (reps.minUtility() * REP_PROMOTION_SLACK);
    }

    private void refreshRepresentatives(RepresentativeSet.Change repChange) {
        if (repChange.membershipChanged || repChange.updatedRepKey >= 0L) {
            reps.refreshUtilities();
            return;
        }

        if (refreshEveryItems > 0 && (admittedSinceStart % refreshEveryItems == 0)) {
            reps.refreshUtilities();
        }
    }

    private RetentionDecision admitWithFreeCapacity(HeapEntry incoming) {
        boolean becameRep = false;
        RepresentativeSet.Change repChange = RepresentativeSet.Change.none();

        if (shouldPromoteToRepresentative(incoming)) {
            repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.representativeUtility);
            becameRep = repChange.changed && (repChange.addedRepKey == incoming.key || repChange.updatedRepKey == incoming.key);
        }

        if (becameRep) {
            incoming.representative = true;
            store.addRepresentative(incoming);
            addToTotalUtility(incoming.utility);

            if (repChange.removedRepKey >= 0L) {
                store.markNonRepresentative(repChange.removedRepKey);
                HeapEntry demoted = store.get(repChange.removedRepKey);
                if (demoted != null) {
                    double oldUtility = demoted.utility;
                    refreshEntryFromScratch(demoted);
                    adjustTotalUtility(oldUtility, demoted.utility);
                    store.onEntryUpdated(demoted);
                    onNonRepresentativeAddedToSample(demoted.key);
                }
            }
        } else {
            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            onNonRepresentativeAddedToSample(incoming.key);
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey, repChange.updatedRepKey);
        refreshRepresentatives(repChange);
        ensureSampleConsistency();

        if (metrics != null) {
            metrics.winRecordAdmitted(incoming.utility);
            if (repChange.membershipChanged) {
                metrics.winRecordRepReplaced();
            }
        }

        return RetentionDecision.admitted(incoming);
    }

    private RetentionDecision admitAtCapacity(HeapEntry incoming) {
        double worstNonRepU = store.minNonRepresentativeUtility();
        boolean canEvictNonRep = hasEvictableNonRepresentative();
        boolean shouldBecomeRep = shouldPromoteToRepresentative(incoming);

        if (!shouldBecomeRep) {
            if (!canEvictNonRep || incoming.utility <= (worstNonRepU * STORE_ADMISSION_SLACK)) {
                if (metrics != null) {
                    metrics.winRecordDropped(incoming.utility);
                }
                return RetentionDecision.dropped();
            }

            HeapEntry out = store.evictWorstNonRepresentative();
            if (out != null) {
                removeFromTotalUtility(out.utility);
                onNonRepresentativeRemovedFromSample(out.key);
            }

            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            onNonRepresentativeAddedToSample(incoming.key);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            ensureSampleConsistency();
            return RetentionDecision.evictedAndAdmitted(out, incoming);
        }

        if (!canEvictNonRep) {
            if (metrics != null) {
                metrics.winRecordDropped(incoming.utility);
            }
            return RetentionDecision.dropped();
        }

        RepresentativeSet.Change repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.representativeUtility);
        boolean becameRep = repChange.changed && (repChange.addedRepKey == incoming.key || repChange.updatedRepKey == incoming.key);

        if (!becameRep) {
            if (incoming.utility <= (worstNonRepU * STORE_ADMISSION_SLACK)) {
                if (metrics != null) {
                    metrics.winRecordDropped(incoming.utility);
                }
                return RetentionDecision.dropped();
            }

            HeapEntry out = store.evictWorstNonRepresentative();
            if (out != null) {
                removeFromTotalUtility(out.utility);
                onNonRepresentativeRemovedFromSample(out.key);
            }

            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            onNonRepresentativeAddedToSample(incoming.key);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            ensureSampleConsistency();
            return RetentionDecision.evictedAndAdmitted(out, incoming);
        }

        incoming.representative = true;
        store.addRepresentative(incoming);
        addToTotalUtility(incoming.utility);

        long demotedRepKey = repChange.removedRepKey;
        if (demotedRepKey >= 0L) {
            store.markNonRepresentative(demotedRepKey);
            HeapEntry demoted = store.get(demotedRepKey);
            if (demoted != null) {
                double oldUtility = demoted.utility;
                refreshEntryFromScratch(demoted);
                adjustTotalUtility(oldUtility, demoted.utility);
                store.onEntryUpdated(demoted);
                onNonRepresentativeAddedToSample(demoted.key);
            }
        }

        HeapEntry out = store.evictWorstNonRepresentativeExcept(demotedRepKey);
        if (out == null) {
            out = store.evictWorstNonRepresentative();
        }
        if (out != null) {
            removeFromTotalUtility(out.utility);
            onNonRepresentativeRemovedFromSample(out.key);
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey, repChange.updatedRepKey);
        refreshRepresentatives(repChange);
        ensureSampleConsistency();

        if (metrics != null) {
            if (out != null) {
                metrics.winRecordEvicted();
            }
            metrics.winRecordAdmitted(incoming.utility);
            if (repChange.membershipChanged) {
                metrics.winRecordRepReplaced();
            }
        }

        if (out == null) {
            return RetentionDecision.admitted(incoming);
        }
        return RetentionDecision.evictedAndAdmitted(out, incoming);
    }

    private boolean hasEvictableNonRepresentative() {
        return store.hasNonRepresentative();
    }

    private void applyRepChanges(long removedRepKey, long addedRepKey, long updatedRepKey) {
        if (store.size() == 0) return;
        if (removedRepKey < 0L && addedRepKey < 0L && updatedRepKey < 0L) return;

        for (HeapEntry e : store.nonRepresentativeEntries()) {
            double oldUtility = e.utility;
            boolean changed = false;

            if (removedRepKey >= 0L) {
                if (e.nearestRepKey == removedRepKey) {
                    e.nearestRepKey = e.secondNearestRepKey;
                    e.nearestDistance = e.secondNearestDistance;

                    RepresentativeSet.UtilityScore replacementSecond = reps.bestExcluding(e.key, e.vector, e.nearestRepKey);
                    e.secondNearestRepKey = replacementSecond.nearestRepKey();
                    e.secondNearestDistance = replacementSecond.utility();
                    changed = true;
                } else if (e.secondNearestRepKey == removedRepKey) {
                    RepresentativeSet.UtilityScore replacementSecond = reps.bestExcluding(e.key, e.vector, e.nearestRepKey);
                    e.secondNearestRepKey = replacementSecond.nearestRepKey();
                    e.secondNearestDistance = replacementSecond.utility();
                    changed = true;
                }
            }

            if (updatedRepKey >= 0L && (e.nearestRepKey == updatedRepKey || e.secondNearestRepKey == updatedRepKey)) {
                refreshEntryFromScratch(e);
                changed = true;
            }

            if (addedRepKey >= 0L && e.key != addedRepKey) {
                double d = reps.distanceToRep(addedRepKey, e.vector);
                if (Double.isFinite(d)) {
                    if (!Double.isFinite(e.nearestDistance) || d < e.nearestDistance) {
                        e.secondNearestRepKey = e.nearestRepKey;
                        e.secondNearestDistance = e.nearestDistance;
                        e.nearestRepKey = addedRepKey;
                        e.nearestDistance = d;
                        changed = true;
                    } else if (addedRepKey != e.nearestRepKey && (!Double.isFinite(e.secondNearestDistance) || d < e.secondNearestDistance)) {
                        e.secondNearestRepKey = addedRepKey;
                        e.secondNearestDistance = d;
                        changed = true;
                    }
                }
            }

            if (e.nearestRepKey < 0L || !Double.isFinite(e.nearestDistance)) {
                refreshEntryFromScratch(e);
                changed = true;
            }

            if (changed) {
                e.utility = computeStoreUtility(e.key, e.vector);
                e.representativeUtility = RepresentativeSet.computeRepresentativeUtility(e.nearestDistance, e.secondNearestDistance);
                adjustTotalUtility(oldUtility, e.utility);
                store.onEntryUpdated(e);
            }
        }

        ensureSampleConsistency();
    }

    private void refreshEntryFromScratch(HeapEntry e) {
        RepresentativeSet.NeighborScore s = reps.scoreWithSecond(e.key, e.vector);
        e.nearestDistance = s.nearestUtility();
        e.nearestRepKey = s.nearestRepKey();
        e.secondNearestDistance = s.secondNearestUtility();
        e.secondNearestRepKey = s.secondNearestRepKey();
        e.utility = computeStoreUtility(e.key, e.vector);
        e.representativeUtility = RepresentativeSet.computeRepresentativeUtility(s.nearestUtility(), s.secondNearestUtility());
    }

    private int targetSampleSize() {
        return Math.max(1, nonRepSampleFactor * Math.max(1, reps.size()));
    }

    private void onNonRepresentativeAddedToSample(long key) {
        HeapEntry e = store.get(key);
        if (e == null || e.representative) {
            return;
        }

        if (sampleContains(key)) {
            return;
        }

        int target = targetSampleSize();
        if (sampledNonRepKeys.size() < target) {
            sampledNonRepKeys.add(key);
            return;
        }

        int nonRepCount = Math.max(1, store.size() - reps.size());
        int draw = ThreadLocalRandom.current().nextInt(nonRepCount);
        if (draw < target) {
            int replaceIdx = ThreadLocalRandom.current().nextInt(target);
            sampledNonRepKeys.set(replaceIdx, key);
        }
    }

    private void onNonRepresentativeRemovedFromSample(long key) {
        for (int i = 0; i < sampledNonRepKeys.size(); i++) {
            if (sampledNonRepKeys.get(i) == key) {
                sampledNonRepKeys.remove(i);
                break;
            }
        }
    }

    private boolean sampleContains(long key) {
        for (long sampledKey : sampledNonRepKeys) {
            if (sampledKey == key) return true;
        }
        return false;
    }

    private void ensureSampleConsistency() {
        if (sampledNonRepKeys.isEmpty() && !store.hasNonRepresentative()) {
            return;
        }

        HashSet<Long> seen = new HashSet<>();
        Iterator<Long> it = sampledNonRepKeys.iterator();
        while (it.hasNext()) {
            long key = it.next();
            HeapEntry e = store.get(key);
            if (e == null || e.representative || !seen.add(key)) {
                it.remove();
            }
        }

        int target = targetSampleSize();
        if (sampledNonRepKeys.size() >= target) {
            while (sampledNonRepKeys.size() > target) {
                sampledNonRepKeys.remove(sampledNonRepKeys.size() - 1);
            }
            return;
        }

        for (HeapEntry e : store.nonRepresentativeEntries()) {
            if (sampledNonRepKeys.size() >= target) break;
            if (seen.add(e.key)) {
                sampledNonRepKeys.add(e.key);
            }
        }
    }

    private void addToTotalUtility(double utility) {
        totalUtility += utilityContribution(utility);
    }

    private void removeFromTotalUtility(double utility) {
        totalUtility -= utilityContribution(utility);
    }

    private void adjustTotalUtility(double oldUtility, double newUtility) {
        totalUtility += utilityContribution(newUtility) - utilityContribution(oldUtility);
    }

    private static double utilityContribution(double utility) {
        return Double.isFinite(utility) ? utility : 0.0;
    }

    @Override
    public int storedCount() {
        return store.size();
    }

    @Override
    public double totalUtility() {
        return totalUtility;
    }

    public int representativeCount() {
        return reps.size();
    }

    public double minUtilityStored() {
        return store.size() == 0 ? Double.NaN : store.minNonRepresentativeUtility();
    }

    public double repsMinUtility() {
        return reps.minUtility();
    }

    public double repsMeanUtility() {
        return reps.meanUtility();
    }
}