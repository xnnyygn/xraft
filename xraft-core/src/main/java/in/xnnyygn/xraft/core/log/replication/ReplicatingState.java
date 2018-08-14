package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public interface ReplicatingState {

    NodeId getNodeId();

    int getNextIndex();

    int getMatchIndex();

    boolean backOffNextIndex();

    boolean advance(int lastEntryIndex);

    boolean isReplicationTarget();

    boolean catchUp(int nextLogIndex);

    boolean isReplicating();

    long getLastReplicatedAt();

    void startReplicating();

    void startReplicating(long replicatedAt);

    void stopReplicating();

}
