package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.node.replication.ReplicatingState;

class GroupMember {

    private final NodeEndpoint endpoint;
    private ReplicatingState replicatingState;
    private boolean major;
    private boolean removing = false;

    GroupMember(NodeEndpoint endpoint) {
        this(endpoint, null, true);
    }

    GroupMember(NodeEndpoint endpoint, ReplicatingState replicatingState, boolean major) {
        this.endpoint = endpoint;
        this.replicatingState = replicatingState;
        this.major = major;
    }

    NodeEndpoint getEndpoint() {
        return endpoint;
    }

    NodeId getId() {
        return endpoint.getId();
    }

    void setReplicatingState(ReplicatingState replicatingState) {
        this.replicatingState = replicatingState;
    }

    boolean isReplicationStateSet() {
        return replicatingState != null;
    }

    public ReplicatingState getReplicatingState() {
        if (replicatingState == null) {
            throw new IllegalStateException("replication state not set");
        }
        return replicatingState;
    }

    public boolean isMajor() {
        return major;
    }

    void setMajor(boolean major) {
        this.major = major;
    }

    public boolean isRemoving() {
        return removing;
    }

    public void setRemoving(boolean removing) {
        this.removing = removing;
    }

    public int getNextIndex() {
        return getReplicatingState().getNextIndex();
    }

    public int getMatchIndex() {
        return getReplicatingState().getMatchIndex();
    }

    public boolean advanceReplicatingState(int lastEntryIndex) {
        return getReplicatingState().advance(lastEntryIndex);
    }

    public boolean backOffNextIndex() {
        return getReplicatingState().backOffNextIndex();
    }

    public void startReplicating() {
        getReplicatingState().startReplicating();
    }

    public void startReplicating(long replicatedAt) {
        getReplicatingState().startReplicating(replicatedAt);
    }

    public boolean isReplicating() {
        return getReplicatingState().isReplicating();
    }

    public boolean isReplicationTarget() {
        return getReplicatingState().isReplicationTarget();
    }

    public void stopReplicating() {
        getReplicatingState().stopReplicating();
    }

    // TODO add test
    public boolean shouldReplicate(long minReplicationInterval) {
        ReplicatingState replicatingState = getReplicatingState();
        return !replicatingState.isReplicating() ||
                System.currentTimeMillis() - replicatingState.getLastReplicatedAt() > minReplicationInterval;
    }

    @Override
    public String toString() {
        return "GroupMember{" +
                "endpoint=" + endpoint +
                ", major=" + major +
                ", removing=" + removing +
                ", replicatingState=" + replicatingState +
                '}';
    }

}
