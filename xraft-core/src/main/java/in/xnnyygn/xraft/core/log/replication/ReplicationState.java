package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public interface ReplicationState {

    NodeId getNodeId();

    int getNextIndex();

    int getMatchIndex();

    boolean backOffNextIndex();

    boolean advance(int lastEntryIndex);

    boolean isReplicationTarget();

    boolean catchUp(int nextLogIndex);

    boolean isReplicating();

    void setReplicating(boolean replicating);

    long getLastReplicatedAt();

    void setLastReplicatedAt(long lastReplicatedAt);

}
