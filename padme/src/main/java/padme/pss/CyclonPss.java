package padme.pss;

import java.util.*;

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

    public void bootstrap(Collection<Integer> allNodeIds) {
        view.clear();
        if (allNodeIds == null) return;

        List<Integer> peers = new ArrayList<>();
        for (int id : allNodeIds) {
            if (id != selfId) peers.add(id);
        }
        Collections.shuffle(peers, rnd);

        int n = Math.min(viewSize, peers.size());
        for (int i = 0; i < n; i++) {
            view.put(peers.get(i), 0);
        }
    }

    public void tickAges() {
        for (var e : new ArrayList<>(view.entrySet())) {
            view.put(e.getKey(), e.getValue() + 1);
        }
    }

    public int selectPartnerOldest() {
        int bestPeer = -1;
        int bestAge = Integer.MIN_VALUE;
        for (var e : view.entrySet()) {
            int age = e.getValue();
            if (age > bestAge) {
                bestAge = age;
                bestPeer = e.getKey();
            }
        }
        return bestPeer;
    }

    public List<PeerDescriptor> buildShuffleBuffer() {
        List<PeerDescriptor> buf = new ArrayList<>(shuffleLength + 1);
        buf.add(new PeerDescriptor(selfId, 0));

        List<Integer> peers = new ArrayList<>(view.keySet());
        Collections.shuffle(peers, rnd);

        int take = Math.min(shuffleLength, peers.size());
        for (int i = 0; i < take; i++) {
            int pid = peers.get(i);
            buf.add(new PeerDescriptor(pid, view.getOrDefault(pid, 0)));
        }
        return buf;
    }

    public void merge(List<PeerDescriptor> received) {
        if (received == null || received.isEmpty()) return;

        for (PeerDescriptor pd : received) {
            if (pd == null) continue;
            if (pd.peerId == selfId) continue;
            int curAge = view.getOrDefault(pd.peerId, Integer.MAX_VALUE);
            if (pd.age < curAge) {
                view.put(pd.peerId, pd.age);
            }
        }

        if (view.size() <= viewSize) return;

        List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(view.entrySet());
        entries.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        for (int i = viewSize; i < entries.size(); i++) {
            view.remove(entries.get(i).getKey());
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