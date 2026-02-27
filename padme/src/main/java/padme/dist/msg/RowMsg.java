package padme.dist.msg;

public final class RowMsg {
    public long key;
    public String[] fields;

    public RowMsg() {}

    public RowMsg(long key, String[] fields) {
        this.key = key;
        this.fields = fields;
    }
}