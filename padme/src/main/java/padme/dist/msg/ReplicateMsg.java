package padme.dist.msg;

import java.util.List;

public final class ReplicateMsg {
    public String type;
    public int from;
    public List<RecordMsg> records;

    public ReplicateMsg() {}

    public ReplicateMsg(int from, List<RecordMsg> records) {
        this.type = "REPLICATE";
        this.from = from;
        this.records = records;
    }
}