package padme.pss;

import java.util.*;

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
            Random r = new Random(rnd.nextLong());
            pss[i] = new CyclonPss(i, viewSize, shuffleLength, r);
            pss[i].bootstrap(allIds);
        }
    }

    public void cycleAll() {
        for (CyclonPss c : pss) c.tickAges();

        List<Integer> order = new ArrayList<>(allIds);
        Collections.shuffle(order, rnd);

        for (int i : order) {
            int j = pss[i].selectPartnerOldest();
            if (j < 0 || j == i) continue;
            if (j >= pss.length) continue;

            List<PeerDescriptor> bufI = pss[i].buildShuffleBuffer();
            List<PeerDescriptor> bufJ = pss[j].buildShuffleBuffer();

            pss[i].merge(bufJ);
            pss[j].merge(bufI);
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