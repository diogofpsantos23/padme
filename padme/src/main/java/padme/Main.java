package padme;

import padme.config.Config;
import padme.config.ConfigLoader;
import padme.run.Runner;

import java.nio.file.Path;

public final class Main {
  public static void main(String[] args) {
    if (args == null || args.length != 2 || !args[0].equals("--config")) {
      throw new IllegalArgumentException("usage: --config <path>");
    }

    Config cfg = ConfigLoader.loadFromFile(Path.of(args[1]));
    Runner.run(cfg);
  }
}