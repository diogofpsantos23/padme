package padme.store;

import padme.model.Record;

public interface KvStore {
    void put(long key, Record item);
    Record get(long key);
    void evict(long key);
    Iterable<Record> values();
}
