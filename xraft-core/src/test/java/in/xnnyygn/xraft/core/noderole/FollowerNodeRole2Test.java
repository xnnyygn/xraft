package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.NullElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.NullScheduledFuture;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class FollowerNodeRole2Test {

    @Test
    public void testIsStableBetween() {
        FollowerNodeRole2 role1 = new FollowerNodeRole2(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        FollowerNodeRole2 role2 = new FollowerNodeRole2(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        Assert.assertTrue(FollowerNodeRole2.isStableBetween(role1, role2));
    }

    @Test
    public void testIsStableBetween2() {
        FollowerNodeRole2 role1 = new FollowerNodeRole2(1, NodeId.of("A"), null, ElectionTimeout.NONE);
        FollowerNodeRole2 role2 = new FollowerNodeRole2(2, NodeId.of("A"), null, ElectionTimeout.NONE);
        Assert.assertFalse(FollowerNodeRole2.isStableBetween(role1, role2));
    }

    @Test
    public void testGetNameAndLeaderId() {
        FollowerNodeRole2 role = new FollowerNodeRole2(1, null, null, ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertNull(state.getLeaderId());
    }

    @Test
    public void testGetNameAndLeaderId2() {
        FollowerNodeRole2 role = new FollowerNodeRole2(1, null, NodeId.of("A"), ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("B"));
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

    @Test
    public void testGetState() {
        FollowerNodeRole2 role = new FollowerNodeRole2(1, NodeId.of("B"), NodeId.of("A"), ElectionTimeout.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(NodeId.of("B"), state.getVotedFor());
        Assert.assertEquals(NodeId.of("A"), state.getLeaderId());
    }

}