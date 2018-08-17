package in.xnnyygn.xraft.core.node.replication;

abstract class AbstractReplicatingState implements ReplicatingState {

    private final boolean replicationTarget;
    private boolean replicating = false;
    private long lastReplicatedAt = 0;

    AbstractReplicatingState(boolean replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    @Override
    public boolean isReplicationTarget() {
        return replicationTarget;
    }

    @Override
    public boolean isReplicating() {
        return replicating;
    }

    @Override
    public long getLastReplicatedAt() {
        return lastReplicatedAt;
    }

    @Override
    public void startReplicating() {
        startReplicating(System.currentTimeMillis());
    }

    @Override
    public void startReplicating(long replicatedAt) {
        replicating = true;
        lastReplicatedAt = replicatedAt;
    }

    @Override
    public void stopReplicating() {
        replicating = false;
    }

}
