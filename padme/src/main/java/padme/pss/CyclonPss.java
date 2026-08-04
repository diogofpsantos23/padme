package padme.pss;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class CyclonPss {
    private final int selfId;
    private final int viewSize;
    private final int shuffleLength;
    private final Random rnd;

    private final Map<Integer, Integer> view = new HashMap<>();

    public CyclonPss(int selfId, int viewSize, int shuffleLength, Random rnd) {
        this.selfId = selfId;
        this.viewSize = Math.max(1, viewSize);
        this.shuffleLength = Math.max(1, shuffleLength);
        this.rnd = (rnd == null) ? new Random() : rnd;
    }

    public void bootstrap(Collection<Integer> initialPeers) {
        view.clear();
        if (initialPeers == null || initialPeers.isEmpty()) return;

        List<Integer> peers = new ArrayList<>();
        for (int id : initialPeers) {
            if (id != selfId && !peers.contains(id)) peers.add(id);
        }
        Collections.shuffle(peers, rnd);

        int n = Math.min(viewSize, peers.size());
        for (int i = 0; i < n; i++) {
            view.put(peers.get(i), 0);
        }
    }

    public void tickAges() {
        for (Map.Entry<Integer, Integer> entry : view.entrySet()) {
            entry.setValue(entry.getValue() + 1);
        }
    }

    public int selectPartnerOldest() {
        int bestPeer = -1;
        int bestAge = Integer.MIN_VALUE;
        List<Integer> tied = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : view.entrySet()) {
            int age = entry.getValue();
            if (age > bestAge) {
                bestAge = age;
                tied.clear();
                tied.add(entry.getKey());
            } else if (age == bestAge) {
                tied.add(entry.getKey());
            }
        }

        if (!tied.isEmpty()) {
            bestPeer = tied.get(rnd.nextInt(tied.size()));
        }
        return bestPeer;
    }

    public List<PeerDescriptor> buildShuffleBuffer() {
        List<PeerDescriptor> buffer = new ArrayList<>(shuffleLength + 1);
        buffer.add(new PeerDescriptor(selfId, 0));

        List<Integer> peers = new ArrayList<>(view.keySet());
        Collections.shuffle(peers, rnd);
        int take = Math.min(shuffleLength, peers.size());
        for (int i = 0; i < take; i++) {
            int peerId = peers.get(i);
            buffer.add(new PeerDescriptor(peerId, view.getOrDefault(peerId, 0)));
        }
        return buffer;
    }

    public void merge(List<PeerDescriptor> received) {
        if (received == null || received.isEmpty()) return;

        for (PeerDescriptor descriptor : received) {
            if (descriptor == null || descriptor.peerId == selfId) continue;

            Integer currentAge = view.get(descriptor.peerId);
            if (currentAge == null || descriptor.age < currentAge) {
                view.put(descriptor.peerId, descriptor.age);
            }
        }

        if (view.size() <= viewSize) return;

        List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(view.entrySet());
        Collections.shuffle(entries, rnd);
        entries.sort(Map.Entry.comparingByValue());

        view.clear();
        int keep = Math.min(viewSize, entries.size());
        for (int i = 0; i < keep; i++) {
            Map.Entry<Integer, Integer> entry = entries.get(i);
            view.put(entry.getKey(), entry.getValue());
        }
    }

    public int[] samplePeers(int k) {
        if (k <= 0 || view.isEmpty()) return new int[0];

        List<Integer> peers = new ArrayList<>(view.keySet());
        Collections.shuffle(peers, rnd);
        int n = Math.min(k, peers.size());
        int[] out = new int[n];
        for (int i = 0; i < n; i++) out[i] = peers.get(i);
        return out;
    }

    public int viewSize() {
        return view.size();
    }
}
