package padme;

import padme.config.Config;
import padme.config.ConfigLoader;
import padme.dist.Args;
import padme.dist.NodeProcess;
import padme.dist.Orchestrator;
import padme.run.Runner;

import java.nio.file.Path;
import java.util.Map;

public final class Main {
  public static void main(String[] args) {
    if (args != null && args.length > 0) {
      String cmd = args[0].trim().toLowerCase();
      if (cmd.equals("orchestrator")) {
        Orchestrator.run(args);
        return;
      }
      if (cmd.equals("node")) {
        NodeProcess.run(args);
        return;
      }
    }

    if (args == null || args.length == 0) {
      throw new IllegalArgumentException("missing --config <path>");
    }

    Map<String, String> m = Args.parse(args, 0);
    String cfgArg = Args.get(m, "config", null);
    if (cfgArg == null || cfgArg.isBlank()) {
      throw new IllegalArgumentException("missing --config <path>");
    }

    Config cfg = ConfigLoader.loadFromFile(Path.of(cfgArg));
    Runner.run(cfg);
  }
}