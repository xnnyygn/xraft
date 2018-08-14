package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import org.junit.Assert;
import org.junit.Test;

public class CandidateNodeRole2Test {

    @Test
    public void testGetRoleNameAndLeaderId() {
        CandidateNodeRole2 role = new CandidateNodeRole2(1, ElectionTimeout.NONE);
        RoleNameAndLeaderId state = role.getNameAndLeaderId(NodeId.of("A"));
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertNull(state.getLeaderId());
    }

    @Test
    public void testGetState() {
        CandidateNodeRole2 role = new CandidateNodeRole2(1, 2, ElectionTimeout.NONE);
        RoleState state = role.getState();
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(2, state.getVotesCount());
    }

}