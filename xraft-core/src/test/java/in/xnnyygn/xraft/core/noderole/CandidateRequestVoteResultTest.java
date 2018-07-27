package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CandidateRequestVoteResultTest {

    private MockNodeRoleContext mockNodeStateContext;

    @Before
    public void setUp() throws Exception {
        this.mockNodeStateContext = new MockNodeRoleContext();
        this.mockNodeStateContext.setConnector(new MockConnector());
        this.mockNodeStateContext.setLog(new MemoryLog());
    }

    @Test
    public void testOnReceiveRequestVoteResultMajor() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeRole candidate = new CandidateNodeRole(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, true));
        AbstractNodeRole nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(RoleName.LEADER, nodeState.getRole());
        Assert.assertEquals(1, nodeState.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultNotMajor() {
        this.mockNodeStateContext.setNodeCount(5);
        CandidateNodeRole candidate = new CandidateNodeRole(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, true));
        AbstractNodeRole nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(RoleName.CANDIDATE, nodeState.getRole());
        Assert.assertEquals(1, nodeState.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultNotGranted() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeRole candidate = new CandidateNodeRole(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, false));
        Assert.assertNull(this.mockNodeStateContext.getNodeState());
    }

    @Test
    public void testOnReceiveRequestVoteResultHigherTerm() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeRole candidate = new CandidateNodeRole(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(2, false));
        AbstractNodeRole nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(RoleName.FOLLOWER, nodeState.getRole());
        Assert.assertEquals(2, nodeState.getTerm());
    }
}