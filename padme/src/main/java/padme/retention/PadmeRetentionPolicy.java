package padme.retention;

import java.util.HashMap;
import java.util.Map;

import padme.metrics.Metrics;
import padme.store.HeapEntry;
import padme.store.HeapItemStore;
import padme.store.ItemStore;

public final class PadmeRetentionPolicy implements RetentionPolicy {
    private static final int BOOTSTRAP_REPRESENTATIVES = 8;
    private static final double STORE_ADMISSION_SLACK = 1.00;
    private static final double REP_PROMOTION_SLACK = 1.10;
    private static final double EPS = 1e-9;

    private final double binBalanceGamma;
    private final double binBalanceMin;
    private final double binBalanceMax;

    private static final class RepStats {
        long load = 0L;
        double sumDistance = 0.0;

        double meanDistance() {
            return load > 0 ? (sumDistance / load) : 0.0;
        }
    }

    private static final class RepStatsDelta {
        long loadDelta = 0L;
        double sumDistanceDelta = 0.0;

        void add(double d) {
            if (!Double.isFinite(d)) return;
            loadDelta++;
            sumDistanceDelta += d;
        }

        void remove(double d) {
            if (!Double.isFinite(d)) return;
            loadDelta--;
            sumDistanceDelta -= d;
        }
    }

    private final ItemStore store;
    private final RepresentativeSet reps;
    private final Metrics metrics;
    private final Map<Long, RepStats> repStats = new HashMap<>();

    private final int refreshEveryItems;
    private long admittedSinceStart = 0;

    private double totalUtility = 0.0;

    public PadmeRetentionPolicy(int maxStoredItems, RepresentativeSet reps, int refreshEveryItems, double binBalanceGamma, double binBalanceMin, double binBalanceMax, Metrics metrics) {
        this.reps = reps;
        this.refreshEveryItems = refreshEveryItems;
        this.binBalanceGamma = binBalanceGamma;
        this.binBalanceMin = binBalanceMin;
        this.binBalanceMax = binBalanceMax;
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
            storeU = computeStoreUtility(score.nearestRepKey(), score.nearestUtility());
            repU = RepresentativeSet.computeRepresentativeUtility(score.nearestUtility(), score.secondNearestUtility());

            if (!Double.isFinite(storeU)) {
                if (metrics != null) {
                    metrics.winRecordSeen(storeU);
                    metrics.winRecordDropped(storeU);
                }
                return RetentionDecision.dropped();
            }
        }

        if (metrics != null) metrics.winRecordSeen(storeU);

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

    private double computeStoreUtility(long nearestRepKey, double nearestDistance) {
        if (!Double.isFinite(nearestDistance)) {
            return Double.POSITIVE_INFINITY;
        }

        double d = Math.max(0.0, nearestDistance);

        if (nearestRepKey < 0L) {
            return d;
        }

        RepStats stats = repStats.get(nearestRepKey);
        if (stats == null) {
            return d;
        }

        double mean = Math.max(EPS, stats.meanDistance());
        long load = Math.max(0L, stats.load);

        double novelty = d / mean;

        double targetLoad = Math.max(1.0, estimateUniformTargetLoad());
        double balance = Math.pow((targetLoad + 1.0) / (load + 1.0), binBalanceGamma);
        balance = clamp(balance, binBalanceMin, binBalanceMax);

        return novelty * balance;
    }

    private int nonRepresentativeCapacity() {
        return Math.max(0, store.capacity() - reps.size());
    }

    private int activeBinCount() {
        return Math.max(1, repStats.size());
    }

    private double estimateUniformTargetLoad() {
        int nonRepCap = nonRepresentativeCapacity();
        int bins = activeBinCount();
        if (nonRepCap <= 0) return 0.0;
        return (double) nonRepCap / bins;
    }

    private static double clamp(double x, double lo, double hi) {
        return Math.max(lo, Math.min(hi, x));
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
        Map<Long, RepStatsDelta> repStatsDelta = new HashMap<>();

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
                    recordMembershipAdd(repStatsDelta, demoted);
                }
            }
        } else {
            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            recordMembershipAdd(repStatsDelta, incoming);
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey, repChange.updatedRepKey, repStatsDelta);
        refreshRepresentatives(repChange);
        applyRepStatsDelta(repStatsDelta, repChange.removedRepKey, repChange.addedRepKey);

        if (metrics != null) {
            metrics.winRecordAdmitted(incoming.utility);
            if (repChange.membershipChanged) metrics.winRecordRepReplaced();
        }

        return RetentionDecision.admitted(incoming);
    }

    private RetentionDecision admitAtCapacity(HeapEntry incoming) {
        double worstNonRepU = store.minNonRepresentativeUtility();
        boolean canEvictNonRep = hasEvictableNonRepresentative();
        boolean shouldBecomeRep = shouldPromoteToRepresentative(incoming);

        if (!shouldBecomeRep) {
            if (!canEvictNonRep || incoming.utility <= (worstNonRepU * STORE_ADMISSION_SLACK)) {
                if (metrics != null) metrics.winRecordDropped(incoming.utility);
                return RetentionDecision.dropped();
            }

            Map<Long, RepStatsDelta> repStatsDelta = new HashMap<>();
            HeapEntry out = store.evictWorstNonRepresentative();
            if (out != null) {
                removeFromTotalUtility(out.utility);
                recordMembershipRemove(repStatsDelta, out);
            }

            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            recordMembershipAdd(repStatsDelta, incoming);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            applyRepStatsDelta(repStatsDelta, -1L, -1L);
            return RetentionDecision.evictedAndAdmitted(out, incoming);
        }

        if (!canEvictNonRep) {
            if (metrics != null) metrics.winRecordDropped(incoming.utility);
            return RetentionDecision.dropped();
        }

        Map<Long, RepStatsDelta> repStatsDelta = new HashMap<>();
        RepresentativeSet.Change repChange = reps.maybeUpdate(incoming.key, incoming.vector, incoming.representativeUtility);
        boolean becameRep = repChange.changed && (repChange.addedRepKey == incoming.key || repChange.updatedRepKey == incoming.key);

        if (!becameRep) {
            if (incoming.utility <= (worstNonRepU * STORE_ADMISSION_SLACK)) {
                if (metrics != null) metrics.winRecordDropped(incoming.utility);
                return RetentionDecision.dropped();
            }

            HeapEntry out = store.evictWorstNonRepresentative();
            if (out != null) {
                removeFromTotalUtility(out.utility);
                recordMembershipRemove(repStatsDelta, out);
            }

            store.addNonRepresentative(incoming);
            addToTotalUtility(incoming.utility);
            recordMembershipAdd(repStatsDelta, incoming);

            if (metrics != null) {
                metrics.winRecordEvicted();
                metrics.winRecordAdmitted(incoming.utility);
            }

            admittedSinceStart++;
            applyRepStatsDelta(repStatsDelta, -1L, -1L);
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
                recordMembershipAdd(repStatsDelta, demoted);
            }
        }

        HeapEntry out = store.evictWorstNonRepresentativeExcept(demotedRepKey);
        if (out == null) {
            out = store.evictWorstNonRepresentative();
        }
        if (out != null) {
            removeFromTotalUtility(out.utility);
            recordMembershipRemove(repStatsDelta, out);
        }

        admittedSinceStart++;
        applyRepChanges(repChange.removedRepKey, repChange.addedRepKey, repChange.updatedRepKey, repStatsDelta);
        refreshRepresentatives(repChange);
        applyRepStatsDelta(repStatsDelta, repChange.removedRepKey, repChange.addedRepKey);

        if (metrics != null) {
            if (out != null) metrics.winRecordEvicted();
            metrics.winRecordAdmitted(incoming.utility);
            if (repChange.membershipChanged) metrics.winRecordRepReplaced();
        }

        if (out == null) {
            return RetentionDecision.admitted(incoming);
        }
        return RetentionDecision.evictedAndAdmitted(out, incoming);
    }

    private boolean hasEvictableNonRepresentative() {
        return store.hasNonRepresentative();
    }

    private void applyRepChanges(long removedRepKey, long addedRepKey, long updatedRepKey, Map<Long, RepStatsDelta> repStatsDelta) {
        if (store.size() == 0) return;
        if (removedRepKey < 0L && addedRepKey < 0L && updatedRepKey < 0L) return;

        for (HeapEntry e : store.nonRepresentativeEntries()) {
            long oldNearestRepKey = e.nearestRepKey;
            double oldNearestDistance = e.nearestDistance;
            double oldUtility = e.utility;
            boolean changed = false;

            if (removedRepKey >= 0L) {
                if (e.nearestRepKey == removedRepKey) {
                    e.nearestRepKey = e.secondNearestRepKey;
                    e.nearestDistance = e.secondNearestDistance;
                    e.utility = computeStoreUtility(e.nearestRepKey, e.nearestDistance);

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
                        e.utility = computeStoreUtility(e.nearestRepKey, d);
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
                e.representativeUtility = RepresentativeSet.computeRepresentativeUtility(e.nearestDistance, e.secondNearestDistance);
                adjustTotalUtility(oldUtility, e.utility);
                recordMembershipTransition(repStatsDelta, oldNearestRepKey, oldNearestDistance, e.nearestRepKey, e.nearestDistance);
                store.onEntryUpdated(e);
            }
        }
    }

    private void refreshEntryFromScratch(HeapEntry e) {
        RepresentativeSet.NeighborScore s = reps.scoreWithSecond(e.key, e.vector);
        e.nearestDistance = s.nearestUtility();
        e.nearestRepKey = s.nearestRepKey();
        e.secondNearestDistance = s.secondNearestUtility();
        e.secondNearestRepKey = s.secondNearestRepKey();
        e.utility = computeStoreUtility(s.nearestRepKey(), s.nearestUtility());
        e.representativeUtility = RepresentativeSet.computeRepresentativeUtility(s.nearestUtility(), s.secondNearestUtility());
    }

    private void applyRepStatsDelta(Map<Long, RepStatsDelta> deltas, long removedRepKey, long addedRepKey) {
        if (removedRepKey >= 0L) {
            repStats.remove(removedRepKey);
        }

        if (addedRepKey >= 0L) {
            repStats.computeIfAbsent(addedRepKey, k -> new RepStats());
        }

        for (Map.Entry<Long, RepStatsDelta> entry : deltas.entrySet()) {
            long repKey = entry.getKey();
            if (repKey < 0L || repKey == removedRepKey) {
                continue;
            }

            RepStats stats = repStats.computeIfAbsent(repKey, k -> new RepStats());
            RepStatsDelta delta = entry.getValue();

            stats.load += delta.loadDelta;
            stats.sumDistance += delta.sumDistanceDelta;

            if (stats.load <= 0L) {
                stats.load = 0L;
                stats.sumDistance = 0.0;
            } else if (stats.sumDistance < 0.0 && stats.sumDistance > -EPS) {
                stats.sumDistance = 0.0;
            }
        }
    }

    private void recordMembershipAdd(Map<Long, RepStatsDelta> deltas, HeapEntry e) {
        if (e == null || e.representative) return;
        recordMembershipAdd(deltas, e.nearestRepKey, e.nearestDistance);
    }

    private void recordMembershipRemove(Map<Long, RepStatsDelta> deltas, HeapEntry e) {
        if (e == null || e.representative) return;
        recordMembershipRemove(deltas, e.nearestRepKey, e.nearestDistance);
    }

    private void recordMembershipTransition(Map<Long, RepStatsDelta> deltas, long oldRepKey, double oldDistance, long newRepKey, double newDistance) {
        boolean oldValid = isValidMembership(oldRepKey, oldDistance);
        boolean newValid = isValidMembership(newRepKey, newDistance);

        if (!oldValid && !newValid) return;
        if (oldValid && newValid && oldRepKey == newRepKey && oldDistance == newDistance) return;

        if (oldValid) {
            recordMembershipRemove(deltas, oldRepKey, oldDistance);
        }
        if (newValid) {
            recordMembershipAdd(deltas, newRepKey, newDistance);
        }
    }

    private void recordMembershipAdd(Map<Long, RepStatsDelta> deltas, long repKey, double distance) {
        if (!isValidMembership(repKey, distance)) return;
        deltas.computeIfAbsent(repKey, k -> new RepStatsDelta()).add(Math.max(0.0, distance));
    }

    private void recordMembershipRemove(Map<Long, RepStatsDelta> deltas, long repKey, double distance) {
        if (!isValidMembership(repKey, distance)) return;
        deltas.computeIfAbsent(repKey, k -> new RepStatsDelta()).remove(Math.max(0.0, distance));
    }

    private static boolean isValidMembership(long repKey, double distance) {
        return repKey >= 0L && Double.isFinite(distance);
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