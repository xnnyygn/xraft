package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public interface ReplicationState {

    NodeId getNodeId();

    int getNextIndex();

    int getMatchIndex();

    void backOffNextIndex();

    boolean advance(int lastEntryIndex);

    boolean isMember();

    void setMember(boolean member);

}
