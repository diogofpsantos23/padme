package padme.dist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public final class CtrlConn {
    public final Socket sock;
    public final DataInputStream in;
    public final DataOutputStream out;

    public CtrlConn(Socket sock) throws IOException {
        this.sock = sock;
        this.in = new DataInputStream(sock.getInputStream());
        this.out = new DataOutputStream(sock.getOutputStream());
    }

    public void close() {
        try { sock.close(); } catch (Exception ignored) {}
    }
}