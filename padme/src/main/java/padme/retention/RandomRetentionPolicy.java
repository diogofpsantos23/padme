package padme.retention;

import padme.store.HeapEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class RandomRetentionPolicy implements RetentionPolicy {
    private final int capacity;
    private final Random rnd;
    private final List<Long> keys;

    private long seen = 0;
    private double totalUtility = 0.0;

    public RandomRetentionPolicy(int maxStoredItems, long seed) {
        this.capacity = Math.max(0, maxStoredItems);
        this.rnd = new Random(seed);
        this.keys = new ArrayList<>(Math.max(1, this.capacity));
    }

    @Override
    public RetentionDecision onItem(long key, float[] vector) {
        seen++;

        if (capacity == 0) {
            return RetentionDecision.dropped();
        }

        double u = 0.0;

        if (keys.size() < capacity) {
            keys.add(key);
            totalUtility += u;
            return RetentionDecision.admitted(new HeapEntry(key, u));
        }

        long j = nextLongBounded(seen);
        if (j >= capacity) {
            return RetentionDecision.dropped();
        }

        int idx = (int) j;
        long evictedKey = keys.set(idx, key);

        HeapEntry out = new HeapEntry(evictedKey, u);
        HeapEntry in = new HeapEntry(key, u);
        return RetentionDecision.evictedAndAdmitted(out, in);
    }

    private long nextLongBounded(long bound) {
        if (bound <= 0) return 0;

        long m = bound - 1;
        if ((bound & m) == 0L) {
            return rnd.nextLong() & m;
        }

        long u;
        long r;
        do {
            u = rnd.nextLong() >>> 1;
            r = u % bound;
        } while (u - r + m < 0L);
        return r;
    }

    @Override
    public int storedCount() {
        return keys.size();
    }

    @Override
    public double totalUtility() {
        return totalUtility;
    }
}