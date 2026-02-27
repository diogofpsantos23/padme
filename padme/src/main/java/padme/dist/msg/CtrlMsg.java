package padme.dist.msg;

import java.util.List;

public final class CtrlMsg {
    public String type;
    public Integer nodeId;
    public Integer round;
    public Integer queueSize;
    public Integer graceMs;
    public String mode;
    public List<RowMsg> rows;

    public CtrlMsg() {}

    public static CtrlMsg hello(int nodeId, String mode) {
        CtrlMsg m = new CtrlMsg();
        m.type = "HELLO";
        m.nodeId = nodeId;
        m.mode = mode;
        return m;
    }

    public static CtrlMsg ingestDone(int nodeId) {
        CtrlMsg m = new CtrlMsg();
        m.type = "INGEST_DONE";
        m.nodeId = nodeId;
        return m;
    }

    public static CtrlMsg startRound(int round) {
        CtrlMsg m = new CtrlMsg();
        m.type = "START_ROUND";
        m.round = round;
        return m;
    }

    public static CtrlMsg roundDone(int nodeId, int round, int queueSize) {
        CtrlMsg m = new CtrlMsg();
        m.type = "ROUND_DONE";
        m.nodeId = nodeId;
        m.round = round;
        m.queueSize = queueSize;
        return m;
    }

    public static CtrlMsg stop(int graceMs) {
        CtrlMsg m = new CtrlMsg();
        m.type = "STOP";
        m.graceMs = graceMs;
        return m;
    }

    public static CtrlMsg collect() {
        CtrlMsg m = new CtrlMsg();
        m.type = "COLLECT";
        return m;
    }

    public static CtrlMsg resultsChunk(int nodeId, List<RowMsg> rows) {
        CtrlMsg m = new CtrlMsg();
        m.type = "RESULTS_CHUNK";
        m.nodeId = nodeId;
        m.rows = rows;
        return m;
    }

    public static CtrlMsg resultsEnd(int nodeId) {
        CtrlMsg m = new CtrlMsg();
        m.type = "RESULTS_END";
        m.nodeId = nodeId;
        return m;
    }
}