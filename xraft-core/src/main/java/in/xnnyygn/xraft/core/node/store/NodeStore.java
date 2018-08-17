package in.xnnyygn.xraft.core.node.store;

import in.xnnyygn.xraft.core.node.NodeId;

// TODO add doc
public interface NodeStore {

    int getTerm();

    void setTerm(int term);

    NodeId getVotedFor();

    void setVotedFor(NodeId votedFor);

    void close();

}
