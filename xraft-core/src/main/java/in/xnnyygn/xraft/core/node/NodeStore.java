package in.xnnyygn.xraft.core.node;

public interface NodeStore {

    int getCurrentTerm();

    void setCurrentTerm(int currentTerm);

    NodeId getVotedFor();

    void setVotedFor(NodeId votedFor);

    void close();

}
