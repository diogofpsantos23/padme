package padme.retention;

import padme.metrics.Metrics;
import padme.store.HeapEntry;
import padme.store.HeapItemStore;
import padme.store.ItemStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public final class GraphCutRetentionPolicy implements RetentionPolicy {
    private static final int BOOTSTRAP_REPRESENTATIVES = 1;
    private static final double STORE_ADMISSION_SLACK = 1.00;
    private static final double REP_PROMOTION_SLACK = 1.10;
    private static final double NUMERIC_EPSILON = 1.0e-12;

    private record ItemVector(long key, float[] vector) {
    }

    private record GraphCutComponents(double coverage, double redundancy) {
    }

    private static final class SampleDelta {
        private final Map<Long, ItemVector> added = new LinkedHashMap<>();
        private final Map<Long, ItemVector> removed = new LinkedHashMap<>();

        void recordAdded(ItemVector item) {
            ItemVector cancelledRemoval = removed.remove(item.key());
            if (cancelledRemoval == null) {
                added.put(item.key(), item);
            }
        }

        void recordRemoved(ItemVector item) {
            ItemVector cancelledAddition = added.remove(item.key());
            if (cancelledAddition == null) {
                removed.put(item.key(), item);
            }
        }

        boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }
    }

    private final ItemStore store;
    private final RepresentativeSet reps;
    private final Metrics metrics;
    private final double lambda;
    private final int nonRepSampleFactor;
    private final ArrayList<ItemVector> sampledNonRepresentatives = new ArrayList<>();

    private long admittedSinceStart = 0;
    private double totalUtility = 0.0;

    public GraphCutRetentionPolicy(int maxStoredItems, RepresentativeSet reps, int refreshEveryItems, double lambda, int nonRepSampleFactor, Metrics metrics) {
        this.reps = reps;
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

        HeapEntry existing = store.get(key);
        if (existing != null) {
            if (metrics != null) {
                metrics.winRecordSeen(existing.utility);
                metrics.winRecordDropped(existing.utility);
            }
            return RetentionDecision.droppedDuplicate();
        }

        RepresentativeSet.NeighborScore score;
        double storeUtility;
        double representativeUtility;
        GraphCutComponents components;

        if (reps.isEmpty()) {
            score = new RepresentativeSet.NeighborScore(-1L, Double.POSITIVE_INFINITY, -1L, Double.POSITIVE_INFINITY);
            components = new GraphCutComponents(0.0, 0.0);
            storeUtility = Double.POSITIVE_INFINITY;
            representativeUtility = Double.POSITIVE_INFINITY;
        } else {
            score = reps.scoreWithSecond(key, vector);
            components = computeGraphCutComponents(key, vector);
            storeUtility = utilityFromComponents(components.coverage(), components.redundancy());
            representativeUtility = RepresentativeSet.computeRepresentativeUtility(score.nearestUtility(), score.secondNearestUtility());

            if (!Double.isFinite(storeUtility)) {
                if (metrics != null) {
                    metrics.winRecordSeen(storeUtility);
                    metrics.winRecordDropped(storeUtility);
                }
                return RetentionDecision.dropped();
            }
        }

        if (metrics != null) {
            metrics.winRecordSeen(storeUtility);
        }

        HeapEntry incoming = new HeapEntry(
                key,
                vector,
                storeUtility,
                representativeUtility,
                score.nearestUtility(),
                score.nearestRepKey(),
                score.secondNearestUtility(),
                score.secondNearestRepKey(),
                false
        );
        incoming.graphCutCoverageSum = components.coverage();
        incoming.graphCutRedundancySum = components.redundancy();

        if (store.size() < store.capacity()) {
            return admitWithFreeCapacity(incoming);
        }

        return admitAtCapacity(incoming);
    }

    private GraphCutComponents computeGraphCutComponents(long itemKey, float[] vector) {
        double coverage = 0.0;
        for (HeapEntry representative : store.representativeEntries()) {
            if (representative.key == itemKey) continue;
            coverage += similarity(vector, representative.vector);
        }

        double redundancy = 0.0;
        for (ItemVector sampled : sampledNonRepresentatives) {
            if (sampled.key() == itemKey) continue;
            redundancy += similarity(vector, sampled.vector());
        }

        return new GraphCutComponents(coverage, redundancy);
    }

    private double utilityFromComponents(double coverage, double redundancy) {
        return (lambda * coverage) - (2.0 * redundancy);
    }

    private double similarity(float[] a, float[] b) {
        if (a == null || b == null) return 0.0;
        double distance = reps.distanceBetween(a, b);
        if (!Double.isFinite(distance)) return 0.0;
        return 1.0 / (1.0 + Math.max(0.0, distance));
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

    private RetentionDecision admitWithFreeCapacity(HeapEntry incoming) {
        RepresentativeSet.Change repChange = RepresentativeSet.Change.none();
        boolean becameRepresentative = false;

        if (shouldPromoteToRepresentative(incoming)) {
            repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.representativeUtility);
            becameRepresentative = repChange.changed
                    && (repChange.addedRepKey == incoming.key || repChange.updatedRepKey == incoming.key);
        }

        if (becameRepresentative) {
            HeapEntry removedRepresentative = repChange.removedRepKey >= 0L ? store.get(repChange.removedRepKey) : null;
            float[] removedRepresentativeVector = removedRepresentative == null ? null : removedRepresentative.vector;

            incoming.representative = true;
            store.addRepresentative(incoming);
            addToTotalUtility(incoming.utility);

            HeapEntry demoted = demoteRepresentative(repChange.removedRepKey);
            boolean utilitiesChanged = applyRepresentativeChange(
                    repChange,
                    removedRepresentativeVector,
                    null,
                    incoming.vector,
                    demoted
            );

            SampleDelta sampleDelta = new SampleDelta();
            if (demoted != null) {
                considerForSample(demoted, sampleDelta);
            }
            reconcileSample(sampleDelta);
            utilitiesChanged |= applySampleDelta(sampleDelta);

            if (utilitiesChanged) {
                store.rebuildHeap();
            }
        } else {
            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);

            SampleDelta sampleDelta = new SampleDelta();
            considerForSample(incoming, sampleDelta);
            reconcileSample(sampleDelta);
            if (applySampleDelta(sampleDelta)) {
                store.rebuildHeap();
            }
        }

        admittedSinceStart++;
        recordAdmissionMetrics(incoming, repChange, null);
        return RetentionDecision.admitted(incoming);
    }

    private RetentionDecision admitAtCapacity(HeapEntry incoming) {
        double worstNonRepresentativeUtility = store.minNonRepresentativeUtility();
        boolean canEvictNonRepresentative = store.hasNonRepresentative();
        boolean shouldBecomeRepresentative = shouldPromoteToRepresentative(incoming);

        if (!shouldBecomeRepresentative) {
            if (!canEvictNonRepresentative || incoming.utility <= (worstNonRepresentativeUtility * STORE_ADMISSION_SLACK)) {
                recordDrop(incoming.utility);
                return RetentionDecision.dropped();
            }
            return replaceWorstNonRepresentative(incoming);
        }

        if (!canEvictNonRepresentative && !store.isRepresentative(incoming.key)) {
            recordDrop(incoming.utility);
            return RetentionDecision.dropped();
        }

        RepresentativeSet.Change repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.representativeUtility);
        boolean becameRepresentative = repChange.changed
                && (repChange.addedRepKey == incoming.key || repChange.updatedRepKey == incoming.key);

        if (!becameRepresentative) {
            if (incoming.utility <= (worstNonRepresentativeUtility * STORE_ADMISSION_SLACK)) {
                recordDrop(incoming.utility);
                return RetentionDecision.dropped();
            }
            return replaceWorstNonRepresentative(incoming);
        }

        HeapEntry removedRepresentative = repChange.removedRepKey >= 0L ? store.get(repChange.removedRepKey) : null;
        float[] removedRepresentativeVector = removedRepresentative == null ? null : removedRepresentative.vector;

        incoming.representative = true;
        store.addRepresentative(incoming);
        addToTotalUtility(incoming.utility);

        HeapEntry demoted = demoteRepresentative(repChange.removedRepKey);
        boolean utilitiesChanged = applyRepresentativeChange(
                repChange,
                removedRepresentativeVector,
                null,
                incoming.vector,
                demoted
        );

        SampleDelta firstSampleDelta = new SampleDelta();
        if (demoted != null) {
            considerForSample(demoted, firstSampleDelta);
        }
        reconcileSample(firstSampleDelta);
        utilitiesChanged |= applySampleDelta(firstSampleDelta);

        if (utilitiesChanged) {
            store.rebuildHeap();
        }

        HeapEntry evicted = null;
        if (store.size() > store.capacity()) {
            evicted = store.evictWorstNonRepresentativeExcept(repChange.removedRepKey);
            if (evicted == null) {
                evicted = store.evictWorstNonRepresentative();
            }

            if (evicted != null) {
                removeFromTotalUtility(evicted.utility);

                SampleDelta evictionSampleDelta = new SampleDelta();
                removeFromSample(evicted.key, evictionSampleDelta);
                reconcileSample(evictionSampleDelta);
                if (applySampleDelta(evictionSampleDelta)) {
                    store.rebuildHeap();
                }
            }
        }

        admittedSinceStart++;
        recordAdmissionMetrics(incoming, repChange, evicted);

        if (evicted == null) {
            return RetentionDecision.admitted(incoming);
        }
        return RetentionDecision.evictedAndAdmitted(evicted, incoming);
    }

    private RetentionDecision replaceWorstNonRepresentative(HeapEntry incoming) {
        HeapEntry evicted = store.evictWorstNonRepresentative();
        if (evicted != null) {
            removeFromTotalUtility(evicted.utility);
        }

        store.addNonRepresentative(incoming);
        addToTotalUtility(incoming.utility);

        SampleDelta sampleDelta = new SampleDelta();
        if (evicted != null) {
            removeFromSample(evicted.key, sampleDelta);
        }
        considerForSample(incoming, sampleDelta);
        reconcileSample(sampleDelta);

        if (applySampleDelta(sampleDelta)) {
            store.rebuildHeap();
        }

        admittedSinceStart++;
        recordAdmissionMetrics(incoming, RepresentativeSet.Change.none(), evicted);
        return RetentionDecision.evictedAndAdmitted(evicted, incoming);
    }

    private HeapEntry demoteRepresentative(long representativeKey) {
        if (representativeKey < 0L) return null;

        store.markNonRepresentative(representativeKey);
        return store.get(representativeKey);
    }

    private boolean applyRepresentativeChange(RepresentativeSet.Change change, float[] removedRepresentativeVector, float[] previousUpdatedVector, float[] newRepresentativeVector, HeapEntry demoted) {
        if (!change.changed) return false;

        boolean changedAnyUtility = false;

        for (HeapEntry entry : store.nonRepresentativeEntries()) {
            if (demoted != null && entry.key == demoted.key) continue;

            double oldUtility = entry.utility;

            if (change.removedRepKey >= 0L && removedRepresentativeVector != null) {
                entry.graphCutCoverageSum -= similarity(entry.vector, removedRepresentativeVector);
            }
            if (change.addedRepKey >= 0L) {
                entry.graphCutCoverageSum += similarity(entry.vector, newRepresentativeVector);
            }
            if (change.updatedRepKey >= 0L) {
                if (previousUpdatedVector != null) {
                    entry.graphCutCoverageSum -= similarity(entry.vector, previousUpdatedVector);
                }
                entry.graphCutCoverageSum += similarity(entry.vector, newRepresentativeVector);
            }

            entry.graphCutCoverageSum = normalizeAccumulatedValue(entry.graphCutCoverageSum);
            updateNeighborMetadata(entry, change);
            entry.utility = utilityFromComponents(entry.graphCutCoverageSum, entry.graphCutRedundancySum);
            adjustTotalUtility(oldUtility, entry.utility);
            changedAnyUtility |= Double.compare(oldUtility, entry.utility) != 0;
        }

        if (demoted != null) {
            double oldUtility = demoted.utility;
            refreshNeighborMetadataFromScratch(demoted);
            initializeGraphCutComponents(demoted);
            adjustTotalUtility(oldUtility, demoted.utility);
            changedAnyUtility = true;
        }

        return changedAnyUtility;
    }

    private void updateNeighborMetadata(HeapEntry entry, RepresentativeSet.Change change) {
        if (change.updatedRepKey >= 0L) {
            refreshNeighborMetadataFromScratch(entry);
            return;
        }

        boolean metadataChanged = false;

        if (change.removedRepKey >= 0L) {
            if (entry.nearestRepKey == change.removedRepKey) {
                entry.nearestRepKey = entry.secondNearestRepKey;
                entry.nearestDistance = entry.secondNearestDistance;
                RepresentativeSet.UtilityScore replacementSecond = reps.bestExcluding(entry.key, entry.vector, entry.nearestRepKey);
                entry.secondNearestRepKey = replacementSecond.nearestRepKey();
                entry.secondNearestDistance = replacementSecond.utility();
                metadataChanged = true;
            } else if (entry.secondNearestRepKey == change.removedRepKey) {
                RepresentativeSet.UtilityScore replacementSecond = reps.bestExcluding(entry.key, entry.vector, entry.nearestRepKey);
                entry.secondNearestRepKey = replacementSecond.nearestRepKey();
                entry.secondNearestDistance = replacementSecond.utility();
                metadataChanged = true;
            }
        }

        if (change.addedRepKey >= 0L && entry.key != change.addedRepKey) {
            double distance = reps.distanceToRep(change.addedRepKey, entry.vector);
            if (Double.isFinite(distance)) {
                if (!Double.isFinite(entry.nearestDistance) || distance < entry.nearestDistance) {
                    entry.secondNearestRepKey = entry.nearestRepKey;
                    entry.secondNearestDistance = entry.nearestDistance;
                    entry.nearestRepKey = change.addedRepKey;
                    entry.nearestDistance = distance;
                    metadataChanged = true;
                } else if (change.addedRepKey != entry.nearestRepKey
                        && (!Double.isFinite(entry.secondNearestDistance) || distance < entry.secondNearestDistance)) {
                    entry.secondNearestRepKey = change.addedRepKey;
                    entry.secondNearestDistance = distance;
                    metadataChanged = true;
                }
            }
        }

        if (entry.nearestRepKey < 0L || !Double.isFinite(entry.nearestDistance)) {
            refreshNeighborMetadataFromScratch(entry);
            return;
        }

        if (metadataChanged) {
            entry.representativeUtility = RepresentativeSet.computeRepresentativeUtility(entry.nearestDistance, entry.secondNearestDistance);
        }
    }

    private void refreshNeighborMetadataFromScratch(HeapEntry entry) {
        RepresentativeSet.NeighborScore score = reps.scoreWithSecond(entry.key, entry.vector);
        entry.nearestDistance = score.nearestUtility();
        entry.nearestRepKey = score.nearestRepKey();
        entry.secondNearestDistance = score.secondNearestUtility();
        entry.secondNearestRepKey = score.secondNearestRepKey();
        entry.representativeUtility = RepresentativeSet.computeRepresentativeUtility(score.nearestUtility(), score.secondNearestUtility());
    }

    private void initializeGraphCutComponents(HeapEntry entry) {
        GraphCutComponents components = computeGraphCutComponents(entry.key, entry.vector);
        entry.graphCutCoverageSum = components.coverage();
        entry.graphCutRedundancySum = components.redundancy();
        entry.utility = utilityFromComponents(components.coverage(), components.redundancy());
    }

    private int targetSampleSize() {
        return Math.max(1, nonRepSampleFactor * Math.max(1, reps.size()));
    }

    private void considerForSample(HeapEntry entry, SampleDelta delta) {
        if (entry == null || entry.representative || sampleContains(entry.key)) return;

        int target = targetSampleSize();
        ItemVector candidate = new ItemVector(entry.key, entry.vector);

        if (sampledNonRepresentatives.size() < target) {
            sampledNonRepresentatives.add(candidate);
            delta.recordAdded(candidate);
            return;
        }

        int nonRepresentativeCount = Math.max(1, store.size() - reps.size());
        int draw = ThreadLocalRandom.current().nextInt(nonRepresentativeCount);
        if (draw < target) {
            int replaceIndex = ThreadLocalRandom.current().nextInt(target);
            ItemVector removed = sampledNonRepresentatives.set(replaceIndex, candidate);
            delta.recordRemoved(removed);
            delta.recordAdded(candidate);
        }
    }

    private void removeFromSample(long key, SampleDelta delta) {
        for (int i = 0; i < sampledNonRepresentatives.size(); i++) {
            ItemVector sampled = sampledNonRepresentatives.get(i);
            if (sampled.key() == key) {
                sampledNonRepresentatives.remove(i);
                delta.recordRemoved(sampled);
                return;
            }
        }
    }

    private boolean sampleContains(long key) {
        for (ItemVector sampled : sampledNonRepresentatives) {
            if (sampled.key() == key) return true;
        }
        return false;
    }

    private void reconcileSample(SampleDelta delta) {
        HashSet<Long> seen = new HashSet<>();

        for (int i = 0; i < sampledNonRepresentatives.size();) {
            ItemVector sampled = sampledNonRepresentatives.get(i);
            HeapEntry stored = store.get(sampled.key());
            if (stored == null || stored.representative || !seen.add(sampled.key())) {
                sampledNonRepresentatives.remove(i);
                delta.recordRemoved(sampled);
            } else {
                i++;
            }
        }

        int target = targetSampleSize();
        while (sampledNonRepresentatives.size() > target) {
            ItemVector removed = sampledNonRepresentatives.remove(sampledNonRepresentatives.size() - 1);
            delta.recordRemoved(removed);
            seen.remove(removed.key());
        }

        if (sampledNonRepresentatives.size() >= target) return;

        for (HeapEntry entry : store.nonRepresentativeEntries()) {
            if (sampledNonRepresentatives.size() >= target) break;
            if (seen.add(entry.key)) {
                ItemVector added = new ItemVector(entry.key, entry.vector);
                sampledNonRepresentatives.add(added);
                delta.recordAdded(added);
            }
        }
    }

    private boolean applySampleDelta(SampleDelta delta) {
        if (delta.isEmpty()) return false;

        boolean changedAnyUtility = false;

        for (HeapEntry entry : store.nonRepresentativeEntries()) {
            double oldUtility = entry.utility;

            for (ItemVector removed : delta.removed.values()) {
                if (entry.key != removed.key()) {
                    entry.graphCutRedundancySum -= similarity(entry.vector, removed.vector());
                }
            }
            for (ItemVector added : delta.added.values()) {
                if (entry.key != added.key()) {
                    entry.graphCutRedundancySum += similarity(entry.vector, added.vector());
                }
            }

            entry.graphCutRedundancySum = normalizeAccumulatedValue(entry.graphCutRedundancySum);
            entry.utility = utilityFromComponents(entry.graphCutCoverageSum, entry.graphCutRedundancySum);
            adjustTotalUtility(oldUtility, entry.utility);
            changedAnyUtility |= Double.compare(oldUtility, entry.utility) != 0;
        }

        return changedAnyUtility;
    }

    private double normalizeAccumulatedValue(double value) {
        if (Math.abs(value) < NUMERIC_EPSILON) return 0.0;
        return value;
    }

    private void recordDrop(double utility) {
        if (metrics != null) {
            metrics.winRecordDropped(utility);
        }
    }

    private void recordAdmissionMetrics(HeapEntry incoming, RepresentativeSet.Change repChange, HeapEntry evicted) {
        if (metrics == null) return;

        if (evicted != null) {
            metrics.winRecordEvicted();
        }
        metrics.winRecordAdmitted(incoming.utility);
        if (repChange.membershipChanged) {
            metrics.winRecordRepReplaced();
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
