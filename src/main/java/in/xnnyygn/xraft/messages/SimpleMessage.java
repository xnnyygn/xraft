package in.xnnyygn.xraft.messages;

public class SimpleMessage implements RaftMessage {

    public enum Kind {
        START_UP,
        ELECTION_TIMEOUT
    }

    private final Kind kind;

    public SimpleMessage(Kind kind) {
        this.kind = kind;
    }

    public Kind getKind() {
        return kind;
    }

    @Override
    public String toString() {
        return "SimpleMessage{" + "kind=" + kind + '}';
    }

}
