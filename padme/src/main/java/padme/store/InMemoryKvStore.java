package padme.store;

import java.util.HashMap;
import java.util.Map;

import padme.model.Record;

public final class InMemoryKvStore implements KvStore {

    private final Map<Long, Record> map = new HashMap<>();

    @Override
    public void put(long key, Record item) {
        map.put(key, item);
    }

    @Override
    public Record get(long key) {
        return map.get(key);
    }

    @Override
    public void evict(long key) {
        map.remove(key);
    }

    @Override
    public Iterable<Record> values() {
        return map.values();
    }
}
