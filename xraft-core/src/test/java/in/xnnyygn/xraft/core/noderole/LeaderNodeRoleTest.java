package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.junit.Assert;
import org.junit.Test;

public class LeaderNodeRoleTest {

    @Test
    public void testGetNameAndLeaderId() {
        LeaderNodeRole role = new LeaderNodeRole(1, LogReplicationTask.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("A"));
        Assert.assertEquals(RoleName.LEADER, state.getRoleName());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

    @Test
    public void testGetState() {
        LeaderNodeRole role = new LeaderNodeRole(1, LogReplicationTask.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(RoleName.LEADER, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
    }

}