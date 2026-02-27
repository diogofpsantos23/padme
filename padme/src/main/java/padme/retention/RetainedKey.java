package padme.retention;

public final class RetainedKey {
    public final long key;
    public final double utility;

    public RetainedKey(long key, double utility) {
        this.key = key;
        this.utility = utility;
    }
}
