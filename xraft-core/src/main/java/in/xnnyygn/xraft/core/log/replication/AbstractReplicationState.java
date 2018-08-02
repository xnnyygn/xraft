package in.xnnyygn.xraft.core.log.replication;

abstract class AbstractReplicationState implements ReplicationState {

    private final boolean replicationTarget;
    private boolean replicating = false;
    private long lastReplicatedAt = 0;

    AbstractReplicationState(boolean replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    @Override
    public boolean isReplicationTarget() {
        return replicationTarget;
    }

    @Override
    public boolean catchUp(int nextLogIndex) {
        return getNextIndex() >= nextLogIndex;
    }

    @Override
    public boolean isReplicating() {
        return replicating;
    }

    @Override
    public void setReplicating(boolean replicating) {
        this.replicating = replicating;
    }

    @Override
    public long getLastReplicatedAt() {
        return lastReplicatedAt;
    }

    @Override
    public void setLastReplicatedAt(long lastReplicatedAt) {
        this.lastReplicatedAt = lastReplicatedAt;
    }

}
