package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CandidateRequestVoteResultTest {

    private MockNodeStateContext mockNodeStateContext;

    @Before
    public void setUp() throws Exception {
        this.mockNodeStateContext = new MockNodeStateContext();
    }

    @Test
    public void testOnReceiveRequestVoteResultMajor() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeState candidate = new CandidateNodeState(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, true));
        AbstractNodeState nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(NodeRole.LEADER, nodeState.getRole());
        Assert.assertEquals(1, nodeState.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultNotMajor() {
        this.mockNodeStateContext.setNodeCount(5);
        CandidateNodeState candidate = new CandidateNodeState(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, true));
        AbstractNodeState nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(NodeRole.CANDIDATE, nodeState.getRole());
        Assert.assertEquals(1, nodeState.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultNotGranted() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeState candidate = new CandidateNodeState(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(1, false));
        Assert.assertNull(this.mockNodeStateContext.getNodeState());
    }

    @Test
    public void testOnReceiveRequestVoteResultHigherTerm() {
        this.mockNodeStateContext.setNodeCount(3);
        CandidateNodeState candidate = new CandidateNodeState(1, 1, this.mockNodeStateContext.scheduleElectionTimeout());
        candidate.onReceiveRequestVoteResult(this.mockNodeStateContext, new RequestVoteResult(2, false));
        AbstractNodeState nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(NodeRole.FOLLOWER, nodeState.getRole());
        Assert.assertEquals(2, nodeState.getTerm());
    }
}