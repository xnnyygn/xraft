package in.xnnyygn.xraft.messages;

public class SimpleMessage implements RaftMessage {

    public enum Kind {
        START_UP,
        ELECTION_TIMEOUT_REGISTRATION,
        ELECTION_TIMEOUT,
        ELECTION_TIMEOUT_DEREGISTRATION,
        ELECTION_TIMEOUT_RESET,
        LOG_REPLICATION,
        TEST
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
