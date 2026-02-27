package padme.dist;

import java.util.HashMap;
import java.util.Map;

public final class Args {
    private Args() {}

    public static Map<String, String> parse(String[] args, int from) {
        Map<String, String> m = new HashMap<>();
        for (int i = from; i < args.length; i++) {
            String a = args[i];
            if (a == null) continue;
            a = a.trim();
            if (!a.startsWith("--")) continue;
            String key = a.substring(2);
            String val = "true";
            if (i + 1 < args.length && args[i + 1] != null && !args[i + 1].startsWith("--")) {
                val = args[++i];
            }
            m.put(key, val);
        }
        return m;
    }

    public static String get(Map<String, String> m, String key, String def) {
        String v = m.get(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    public static int getInt(Map<String, String> m, String key, int def) {
        String v = get(m, key, null);
        if (v == null) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    public static long getLong(Map<String, String> m, String key, long def) {
        String v = get(m, key, null);
        if (v == null) return def;
        try { return Long.parseLong(v.trim()); } catch (Exception e) { return def; }
    }
}