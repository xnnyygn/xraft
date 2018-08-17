package in.xnnyygn.xraft.core.node.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public interface ReplicatingState {

    NodeId getNodeId();

    int getNextIndex();

    int getMatchIndex();

    boolean backOffNextIndex();

    boolean advance(int lastEntryIndex);

    // TODO rename to isTarget
    boolean isReplicationTarget();

    boolean isReplicating();

    long getLastReplicatedAt();

    // TODO change to POJO
    void startReplicating();

    void startReplicating(long replicatedAt);

    void stopReplicating();

}
