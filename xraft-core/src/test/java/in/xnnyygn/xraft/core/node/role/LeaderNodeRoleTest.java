package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
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

    @Test
    public void testStateEquals() {
        LeaderNodeRole role1 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        LeaderNodeRole role2 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        Assert.assertTrue(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentTerm() {
        LeaderNodeRole role1 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        LeaderNodeRole role2 = new LeaderNodeRole(2, LogReplicationTask.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentRoleName() {
        LeaderNodeRole role1 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        CandidateNodeRole role2 = new CandidateNodeRole(2, ElectionTimeout.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

}