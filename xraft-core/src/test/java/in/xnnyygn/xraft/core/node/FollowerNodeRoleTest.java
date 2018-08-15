package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.node.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import org.junit.Assert;
import org.junit.Test;

public class FollowerNodeRoleTest {

    @Test
    public void testIsStableBetween() {
        FollowerNodeRole role1 = new FollowerNodeRole(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        FollowerNodeRole role2 = new FollowerNodeRole(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        Assert.assertTrue(FollowerNodeRole.isStableBetween(role1, role2));
    }

    @Test
    public void testIsStableBetween2() {
        FollowerNodeRole role1 = new FollowerNodeRole(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        FollowerNodeRole role2 = new FollowerNodeRole(2, NodeId.of("A"), null, ElectionTimeout.NONE);
        Assert.assertFalse(FollowerNodeRole.isStableBetween(role1, role2));
    }

    @Test
    public void testGetNameAndLeaderId() {
        FollowerNodeRole role = new FollowerNodeRole(1, null, null, ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertNull(state.getLeaderId());
    }

    @Test
    public void testGetNameAndLeaderId2() {
        FollowerNodeRole role = new FollowerNodeRole(1, null, NodeId.of("A"), ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

    @Test
    public void testGetState() {
        FollowerNodeRole role = new FollowerNodeRole(1, NodeId.of("B"), NodeId.of("A"), ElectionTimeout.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(NodeId.of("B"), state.getVotedFor());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

}