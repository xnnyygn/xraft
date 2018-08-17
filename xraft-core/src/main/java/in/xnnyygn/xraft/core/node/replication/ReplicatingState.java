package in.xnnyygn.xraft.core.node.replication;

import in.xnnyygn.xraft.core.node.NodeId;

// TODO add doc
public interface ReplicatingState {

    NodeId getNodeId();

    int getNextIndex();

    int getMatchIndex();

    boolean backOffNextIndex();

    boolean advance(int lastEntryIndex);

    boolean isTarget();

    boolean isReplicating();

    long getLastReplicatedAt();

    void setReplicating(boolean replicating);

    void setLastReplicatedAt(long lastReplicatedAt);

}
