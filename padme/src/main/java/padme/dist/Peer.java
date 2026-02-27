package padme.dist;

public final class Peer {
    public final int id;
    public final String host;
    public final int port;

    public Peer(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public static Peer parse(String s) {
        String t = (s == null) ? "" : s.trim();
        String[] parts = t.split(":");
        if (parts.length < 3) throw new IllegalArgumentException("Invalid peer: " + s);
        int id = Integer.parseInt(parts[0]);
        String host = parts[1];
        int port = Integer.parseInt(parts[2]);
        return new Peer(id, host, port);
    }

    public String encode() {
        return id + ":" + host + ":" + port;
    }
}