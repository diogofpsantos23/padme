package padme.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import padme.model.Record;

public final class RetainedDatasetWriter {
    private RetainedDatasetWriter() {}

    public static void writeSnapshotCsv(Path out, String[] headerFields, Iterable<Record> records) throws IOException {
        Files.createDirectories(out.getParent());

        try (BufferedWriter w = Files.newBufferedWriter(
                out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            if (headerFields != null && headerFields.length > 0) {
                w.write(joinCsv(headerFields));
                w.newLine();
            }

            for (Record r : records) {
                String[] fields = r.item.fields;
                w.write(joinCsv(fields));
                w.newLine();
            }
        }
    }

    private static String joinCsv(String[] fields) {
        StringBuilder sb = new StringBuilder(fields.length * 8);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(escapeCsv(fields[i]));
        }
        return sb.toString();
    }

    private static String escapeCsv(String s) {
        if (s == null) return "";
        boolean needsQuotes = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuotes) return s;
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}