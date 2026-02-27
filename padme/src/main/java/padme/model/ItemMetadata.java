package padme.model;

public final class ItemMetadata {
    public final long version;
    public final float[] vector;
    public final double utility;

    public ItemMetadata(long version, float[] vector, double utility) {
        this.version = version;
        this.vector = vector;
        this.utility = utility;
    }

    public long getVersion() {
        return version;
    }

    public float[] getVector() {
        return vector;
    }

    public double getUtility() {
        return utility;
    }
}
