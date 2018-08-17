package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.role.CandidateNodeRole;
import in.xnnyygn.xraft.core.node.role.RoleName;
import in.xnnyygn.xraft.core.node.role.RoleNameAndLeaderId;
import in.xnnyygn.xraft.core.node.role.RoleState;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
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

}