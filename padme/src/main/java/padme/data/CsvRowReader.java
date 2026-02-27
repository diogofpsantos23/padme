package padme.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CsvRowReader implements AutoCloseable {
  private final BufferedReader br;
  private final char sep;
  private final String[] header;

  public CsvRowReader(String path, String separator, boolean hasHeader) throws IOException {
    this.br = open(path);
    this.sep = (separator == null || separator.isEmpty()) ? ',' : separator.charAt(0);

    if (hasHeader) {
      String headerLine = br.readLine();
      this.header = (headerLine == null) ? new String[0] : splitLine(headerLine);
    } else {
      this.header = new String[0];
    }
  }

  private static BufferedReader open(String path) throws IOException {
    Path p = Path.of(path);
    if (Files.exists(p) && Files.isRegularFile(p)) {
      return Files.newBufferedReader(p, StandardCharsets.UTF_8);
    }

    InputStream in = CsvRowReader.class.getClassLoader().getResourceAsStream(path);
    if (in == null) throw new IOException("CSV not found as file or resource: " + path);

    return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  private String[] splitLine(String line) {
    String t = stripBom(line);
    return fastSplit(t, sep);
  }

  private static String stripBom(String s) {
    if (s == null || s.isEmpty()) return s;
    return (s.charAt(0) == '\uFEFF') ? s.substring(1) : s;
  }

  private static String[] fastSplit(String s, char sep) {
    int n = s.length();
    int count = 1;
    for (int i = 0; i < n; i++) if (s.charAt(i) == sep) count++;

    String[] out = new String[count];
    int k = 0;
    int start = 0;

    for (int i = 0; i < n; i++) {
      if (s.charAt(i) == sep) {
        out[k++] = s.substring(start, i);
        start = i + 1;
      }
    }
    out[k] = s.substring(start);
    return out;
  }

  public String[] header() { return header; }

  public String[] nextRow() throws IOException {
    String line = br.readLine();
    if (line == null) return null;
    return splitLine(line);
  }

  @Override
  public void close() throws IOException {
    br.close();
  }
}