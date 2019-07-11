package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.junit.Assert;
import org.junit.Test;

public class FollowerNodeRoleTest {

    @Test
    public void testGetNameAndLeaderId() {
        FollowerNodeRole role = new FollowerNodeRole(1, null, null, 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertNull(state.getLeaderId());
    }

    @Test
    public void testGetNameAndLeaderId2() {
        FollowerNodeRole role = new FollowerNodeRole(1, null, NodeId.of("A"), 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

    @Test
    public void testGetState() {
        FollowerNodeRole role = new FollowerNodeRole(1, NodeId.of("B"), NodeId.of("A"), 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(NodeId.of("B"), state.getVotedFor());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

    @Test
    public void testStateEquals() {
        FollowerNodeRole role1 = new FollowerNodeRole(1, NodeId.of("A"), null, 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        FollowerNodeRole role2 = new FollowerNodeRole(1, NodeId.of("A"), null, 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        Assert.assertTrue(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentLeaderId() {
        FollowerNodeRole role1 = new FollowerNodeRole(1, NodeId.of("A"), null, 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        FollowerNodeRole role2 = new FollowerNodeRole(1, NodeId.of("A"), NodeId.of("A"), 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentRoleName() {
        FollowerNodeRole role1 = new FollowerNodeRole(1, NodeId.of("A"), null, 0, System.currentTimeMillis(), ElectionTimeout.NONE);
        LeaderNodeRole role2 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

}