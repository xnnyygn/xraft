package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.junit.Assert;
import org.junit.Test;

public class CandidateNodeRoleTest {

    @Test
    public void testGetRoleNameAndLeaderId() {
        CandidateNodeRole role = new CandidateNodeRole(1, ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("A"));
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertNull(state.getLeaderId());
    }

    @Test
    public void testGetState() {
        CandidateNodeRole role = new CandidateNodeRole(1, 2, ElectionTimeout.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(2, state.getVotesCount());
    }

    @Test
    public void testStateEquals() {
        CandidateNodeRole role1 = new CandidateNodeRole(1, 1, ElectionTimeout.NONE);
        CandidateNodeRole role2 = new CandidateNodeRole(1, 1, ElectionTimeout.NONE);
        Assert.assertTrue(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentVotesCount() {
        CandidateNodeRole role1 = new CandidateNodeRole(1, 1, ElectionTimeout.NONE);
        CandidateNodeRole role2 = new CandidateNodeRole(1, 2, ElectionTimeout.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

    @Test
    public void testStateEqualsDifferentRole() {
        CandidateNodeRole role1 = new CandidateNodeRole(1, 1, ElectionTimeout.NONE);
        LeaderNodeRole role2 = new LeaderNodeRole(1, LogReplicationTask.NONE);
        Assert.assertFalse(role1.stateEquals(role2));
    }

}