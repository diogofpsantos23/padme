package padme.model;

public final class Record {
    public final long key;
    public final DataItem item;
    public final ItemMetadata meta;

    public Record(long key, DataItem item, ItemMetadata meta) {
        this.key = key;
        this.item = item;
        this.meta = meta;
    }

    public long getKey() {
        return key;
    }

    public DataItem getItem() {
        return item;
    }

    public ItemMetadata getMetadata() {
        return meta;
    }
}
