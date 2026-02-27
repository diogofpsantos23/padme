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
        if (reps.isEmpty()) {
            if (store.capacity() == 0) {
                if (metrics != null) {
                    metrics.winRecordSeen(0.0);
                    metrics.winRecordDropped(0.0);
                }
                return RetentionDecision.dropped();
            }

            double uBootstrap = Double.POSITIVE_INFINITY;

            if (metrics != null) metrics.winRecordSeen(uBootstrap);

            HeapEntry in = new HeapEntry(key, uBootstrap);
            store.add(in);

            boolean repChanged = reps.maybeUpdate(vector, uBootstrap);
            if (metrics != null) {
                metrics.winRecordAdmitted(uBootstrap);
                if (repChanged) metrics.winRecordRepReplaced();
            }

            admittedSinceStart++;
            if (repChanged) maybeRefreshReps();

            if (Double.isFinite(uBootstrap)) totalUtility += uBootstrap;

            return RetentionDecision.admitted(in);
        }

        double u = reps.utility(vector);

        if (!Double.isFinite(u)) {
            if (metrics != null) {
                metrics.winRecordSeen(u);
                metrics.winRecordDropped(u);
            }
            return RetentionDecision.dropped();
        }

        if (metrics != null) metrics.winRecordSeen(u);

        if (store.capacity() == 0) {
            if (metrics != null) metrics.winRecordDropped(u);
            return RetentionDecision.dropped();
        }

        HeapEntry in = new HeapEntry(key, u);

        if (store.size() < store.capacity()) {
            store.add(in);

            boolean repChanged = reps.maybeUpdate(vector, u);
            if (metrics != null) {
                metrics.winRecordAdmitted(u);
                if (repChanged) metrics.winRecordRepReplaced();
            }

            admittedSinceStart++;
            if (repChanged) maybeRefreshReps();

            totalUtility += u;

            return RetentionDecision.admitted(in);
        }

        double worstU = store.minUtility();
        if (u <= worstU) {
            if (metrics != null) metrics.winRecordDropped(u);
            return RetentionDecision.dropped();
        }

        HeapEntry out = store.evictWorst();
        store.add(in);

        boolean repChanged = reps.maybeUpdate(vector, u);
        if (metrics != null) {
            metrics.winRecordEvicted();
            metrics.winRecordAdmitted(u);
            if (repChanged) metrics.winRecordRepReplaced();
        }

        admittedSinceStart++;
        if (repChanged) maybeRefreshReps();

        if (Double.isFinite(out.utility)) totalUtility -= out.utility;
        totalUtility += u;

        return RetentionDecision.evictedAndAdmitted(out, in);
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

    public double minUtilityStored() { return store.size() == 0 ? Double.NaN : store.minUtility(); }

    public double repsMinUtility() { return reps.minUtility(); }
    public double repsMeanUtility() { return reps.meanUtility(); }
}