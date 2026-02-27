package padme.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {
  private static final ObjectMapper OM = new ObjectMapper();

  private ConfigLoader() {}

  public static Config loadDefault() {
    return loadFromResource("config.json");
  }

  public static Config loadFromFile(Path path) {
    try (InputStream in = Files.newInputStream(path)) {
      Config cfg = OM.readValue(in, Config.class);
      cfg.validate();
      return cfg;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config from file: " + path, e);
    }
  }

  public static void writeToFile(Config cfg, Path path) {
    try {
      Files.createDirectories(path.getParent());
      OM.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), cfg);
    } catch (Exception e) {
      throw new RuntimeException("Failed to write config to file: " + path, e);
    }
  }

  private static Config loadFromResource(String resourcePath) {
    try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (in == null) throw new IllegalStateException("Resource not found: " + resourcePath);

      Config cfg = OM.readValue(in, Config.class);
      cfg.validate();
      return cfg;

    } catch (Exception e) {
      throw new RuntimeException("Failed to load config from resource: " + resourcePath, e);
    }
  }
}