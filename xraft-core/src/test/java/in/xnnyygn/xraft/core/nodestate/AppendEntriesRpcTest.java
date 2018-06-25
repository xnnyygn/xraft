package in.xnnyygn.xraft.core.nodestate;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AppendEntriesRpcTest {

    private Log log;
    private MockConnector mockConnector;
    private MockNodeStateContext mockNodeStateContext;

    @Before
    public void setUp() throws Exception {
        this.log = new MemoryLog(new EventBus());
        this.mockConnector = new MockConnector();

        this.mockNodeStateContext = new MockNodeStateContext();
        this.mockNodeStateContext.setLog(this.log);
        this.mockNodeStateContext.setConnector(this.mockConnector);
    }

    @Test
    public void testOnReceiveAppendEntriesRpcFollowerLowerTerm() {
        NodeId leaderId = new NodeId("L");
        FollowerNodeState follower = new FollowerNodeState(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(1);
        rpc.setLeaderId(leaderId);

        follower.onReceiveAppendEntriesRpc(this.mockNodeStateContext, rpc);

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcFollowerHigherTerm() {
        NodeId leaderId = new NodeId("L");
        FollowerNodeState follower = new FollowerNodeState(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(3);
        rpc.setLeaderId(leaderId);

        follower.onReceiveAppendEntriesRpc(this.mockNodeStateContext, rpc);

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertTrue(result.isSuccess());
        NodeStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(NodeRole.FOLLOWER, snapshot.getRole());
        Assert.assertEquals(3, snapshot.getTerm());
        Assert.assertEquals(leaderId, snapshot.getLeaderId());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcCandidate() {
        NodeId leaderId = new NodeId("L");
        CandidateNodeState candidate = new CandidateNodeState(2, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setLeaderId(leaderId);

        candidate.onReceiveAppendEntriesRpc(this.mockNodeStateContext, rpc);

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertTrue(result.isSuccess());
        NodeStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(NodeRole.FOLLOWER, snapshot.getRole());
        Assert.assertEquals(2, snapshot.getTerm());
        Assert.assertEquals(leaderId, snapshot.getLeaderId());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcLeader() {
        NodeId leaderId = new NodeId("L");
        LeaderNodeState leader = new LeaderNodeState(2, this.mockNodeStateContext.scheduleLogReplicationTask(), null);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setLeaderId(leaderId);

        leader.onReceiveAppendEntriesRpc(this.mockNodeStateContext, rpc);
        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isSuccess());
    }

}
