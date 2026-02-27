package padme.dist.net;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class FramedJson {
    private static final ObjectMapper OM = new ObjectMapper();

    private FramedJson() {}

    public static void write(DataOutputStream out, Object msg) throws IOException {
        byte[] bytes = OM.writeValueAsBytes(msg);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    public static <T> T read(DataInputStream in, Class<T> clazz) throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (len <= 0 || len > 256_000_000) throw new IOException("Invalid frame length: " + len);

        byte[] buf = new byte[len];
        in.readFully(buf);
        return OM.readValue(buf, clazz);
    }

    public static String toJson(Object msg) {
        try {
            return OM.writeValueAsString(msg);
        } catch (Exception e) {
            return "";
        }
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}