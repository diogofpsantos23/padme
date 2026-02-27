package padme.pss;

public final class PeerDescriptor {
    public final int peerId;
    public final int age;

    public PeerDescriptor(int peerId, int age) {
        this.peerId = peerId;
        this.age = age;
    }

    public PeerDescriptor withAge(int newAge) {
        return new PeerDescriptor(peerId, newAge);
    }

    @Override
    public String toString() {
        return "Peer{" + peerId + ", age=" + age + "}";
    }
}