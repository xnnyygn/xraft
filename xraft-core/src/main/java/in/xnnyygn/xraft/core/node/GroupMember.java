package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.node.replication.ReplicatingState;

/**
 * State of group member.
 * @see ReplicatingState
 */
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

    private ReplicatingState ensureReplicatingState() {
        if (replicatingState == null) {
            throw new IllegalStateException("replication state not set");
        }
        return replicatingState;
    }

    boolean isMajor() {
        return major;
    }

    void setMajor(boolean major) {
        this.major = major;
    }

    boolean isRemoving() {
        return removing;
    }

    void setRemoving() {
        removing = true;
    }

    int getNextIndex() {
        return ensureReplicatingState().getNextIndex();
    }

    int getMatchIndex() {
        return ensureReplicatingState().getMatchIndex();
    }

    boolean advanceReplicatingState(int lastEntryIndex) {
        return ensureReplicatingState().advance(lastEntryIndex);
    }

    boolean backOffNextIndex() {
        return ensureReplicatingState().backOffNextIndex();
    }

    void replicateNow() {
        replicateAt(System.currentTimeMillis());
    }

    void replicateAt(long replicatedAt) {
        ReplicatingState replicatingState = ensureReplicatingState();
        replicatingState.setReplicating(true);
        replicatingState.setLastReplicatedAt(replicatedAt);
    }

    boolean isReplicating() {
        return ensureReplicatingState().isReplicating();
    }

    boolean isReplicationTarget() {
        return ensureReplicatingState().isTarget();
    }

    void stopReplicating() {
        ensureReplicatingState().setReplicating(false);
    }

    // TODO add test
    boolean shouldReplicate(long minReplicationInterval) {
        ReplicatingState replicatingState = ensureReplicatingState();
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
