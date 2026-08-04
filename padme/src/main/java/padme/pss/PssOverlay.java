package padme.pss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public final class PssOverlay {
    private final CyclonPss[] pss;
    private final List<Integer> allIds;
    private final Random rnd;

    public PssOverlay(int numNodes, int viewSize, int shuffleLength, long seed) {
        if (numNodes <= 0) throw new IllegalArgumentException("numNodes must be > 0");

        this.rnd = new Random(seed);
        this.pss = new CyclonPss[numNodes];
        this.allIds = new ArrayList<>(numNodes);

        for (int i = 0; i < numNodes; i++) allIds.add(i);
        for (int i = 0; i < numNodes; i++) {
            pss[i] = new CyclonPss(i, viewSize, shuffleLength, new Random(rnd.nextLong()));
        }

        bootstrapConnectedPartialViews(viewSize);
    }

    private void bootstrapConnectedPartialViews(int requestedViewSize) {
        int n = pss.length;
        if (n <= 1) {
            pss[0].bootstrap(List.of());
            return;
        }

        int targetSize = Math.min(Math.max(1, requestedViewSize), n - 1);
        for (int nodeId = 0; nodeId < n; nodeId++) {
            Set<Integer> peers = new LinkedHashSet<>(targetSize);

            addIfPossible(peers, (nodeId + 1) % n, nodeId, targetSize);
            addIfPossible(peers, (nodeId - 1 + n) % n, nodeId, targetSize);

            while (peers.size() < targetSize) {
                addIfPossible(peers, rnd.nextInt(n), nodeId, targetSize);
            }

            pss[nodeId].bootstrap(peers);
        }
    }

    private static void addIfPossible(Set<Integer> peers, int candidate, int selfId, int targetSize) {
        if (peers.size() >= targetSize || candidate == selfId) return;
        peers.add(candidate);
    }

    public void cycleAll() {
        for (CyclonPss cyclon : pss) cyclon.tickAges();

        List<Integer> order = new ArrayList<>(allIds);
        Collections.shuffle(order, rnd);
        for (int nodeId : order) {
            int partnerId = pss[nodeId].selectPartnerOldest();
            if (partnerId < 0 || partnerId == nodeId || partnerId >= pss.length) continue;

            List<PeerDescriptor> nodeBuffer = pss[nodeId].buildShuffleBuffer();
            List<PeerDescriptor> partnerBuffer = pss[partnerId].buildShuffleBuffer();

            pss[nodeId].merge(partnerBuffer);
            pss[partnerId].merge(nodeBuffer);
        }
    }

    public int[] samplePeers(int selfId, int k) {
        if (selfId < 0 || selfId >= pss.length) return new int[0];
        return pss[selfId].samplePeers(k);
    }

    public int numNodes() {
        return pss.length;
    }
}
