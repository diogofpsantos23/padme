package padme.dist.msg;

public final class AckMsg {
    public String type;
    public int from;

    public AckMsg() {}

    public AckMsg(int from) {
        this.type = "ACK";
        this.from = from;
    }
}