package padme.dist;

public final class Hashing {
    private Hashing() {}

    public static int murmur3_32(long x) {
        int h1 = 0;
        int k1 = (int) x;
        k1 *= 0xcc9e2d51;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= 0x1b873593;
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        int k2 = (int) (x >>> 32);
        k2 *= 0xcc9e2d51;
        k2 = Integer.rotateLeft(k2, 15);
        k2 *= 0x1b873593;
        h1 ^= k2;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        h1 ^= 8;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    public static int bucket(long key, int buckets) {
        int h = murmur3_32(key);
        return Math.floorMod(h, buckets);
    }
}