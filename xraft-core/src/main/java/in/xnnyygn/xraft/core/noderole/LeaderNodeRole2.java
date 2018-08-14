package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;

public class LeaderNodeRole2 extends AbstractNodeRole2 {

    private final LogReplicationTask logReplicationTask;

    public LeaderNodeRole2(int term, LogReplicationTask logReplicationTask) {
        super(RoleName.LEADER, term);
        this.logReplicationTask = logReplicationTask;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return selfId;
    }

    @Override
    public void cancelTimeoutOrTask() {
        logReplicationTask.cancel();
    }

    @Override
    public RoleState getState() {
        return new RoleState(RoleName.LEADER, term);
    }

    @Override
    public String toString() {
        return "LeaderNodeRole2{term=" + term + ", logReplicationTask=" + logReplicationTask + '}';
    }
}
