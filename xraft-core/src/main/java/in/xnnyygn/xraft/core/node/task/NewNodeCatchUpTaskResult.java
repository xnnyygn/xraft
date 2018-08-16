package in.xnnyygn.xraft.core.node.task;

public class NewNodeCatchUpTaskResult {

    public static enum State {
        OK,
        TIMEOUT,
        REPLICATION_FAILED
    }

    private final State state;
    private final int nextIndex;
    private final int matchIndex;

    public NewNodeCatchUpTaskResult(State state) {
        this.state = state;
        this.nextIndex = 0;
        this.matchIndex = 0;
    }

    public NewNodeCatchUpTaskResult(int nextIndex, int matchIndex) {
        this.state = State.OK;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    public State getState() {
        return state;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

}