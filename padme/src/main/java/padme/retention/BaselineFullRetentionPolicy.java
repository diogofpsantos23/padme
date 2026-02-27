package padme.retention;

import padme.store.HeapEntry;

public final class BaselineFullRetentionPolicy implements RetentionPolicy {

    private long storedCount = 0;
    private double totalUtility = 0.0;

    @Override
    public RetentionDecision onItem(long key, float[] vector) {
        double u = 0.0;

        storedCount++;
        totalUtility += u;

        return RetentionDecision.admitted(new HeapEntry(key, u));
    }

    @Override
    public int storedCount() {
        return (int) storedCount;
    }

    @Override
    public double totalUtility() {
        return totalUtility;
    }
}
