package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.DefaultLog;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpcMessage;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AppendEntriesRpcTest {

    private Log log;
    private MockConnector mockConnector;
    private MockNodeRoleContext mockNodeStateContext;

    @Before
    public void setUp() {
        this.log = new DefaultLog();
        this.mockConnector = new MockConnector();

        this.mockNodeStateContext = new MockNodeRoleContext();
        this.mockNodeStateContext.setLog(this.log);
        this.mockNodeStateContext.setConnector(this.mockConnector);
    }

    @Test
    public void testOnReceiveAppendEntriesRpcFollowerLowerTerm() {
        NodeId leaderId = new NodeId("L");
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(1);
        rpc.setLeaderId(leaderId);

        follower.onReceiveAppendEntriesRpc(this.mockNodeStateContext, new AppendEntriesRpcMessage(rpc, leaderId, null));

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcFollowerHigherTerm() {
        NodeId leaderId = new NodeId("L");
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(3);
        rpc.setLeaderId(leaderId);

        follower.onReceiveAppendEntriesRpc(this.mockNodeStateContext, new AppendEntriesRpcMessage(rpc, leaderId, null));

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertTrue(result.isSuccess());
        RoleStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(RoleName.FOLLOWER, snapshot.getRole());
        Assert.assertEquals(3, snapshot.getTerm());
        Assert.assertEquals(leaderId, snapshot.getLeaderId());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcCandidate() {
        NodeId leaderId = new NodeId("L");
        CandidateNodeRole candidate = new CandidateNodeRole(2, this.mockNodeStateContext.scheduleElectionTimeout());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setLeaderId(leaderId);

        candidate.onReceiveAppendEntriesRpc(this.mockNodeStateContext, new AppendEntriesRpcMessage(rpc, leaderId, null));

        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertTrue(result.isSuccess());
        RoleStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(RoleName.FOLLOWER, snapshot.getRole());
        Assert.assertEquals(2, snapshot.getTerm());
        Assert.assertEquals(leaderId, snapshot.getLeaderId());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcLeader() {
        NodeId leaderId = new NodeId("L");
        LeaderNodeRole leader = new LeaderNodeRole(2, this.mockNodeStateContext.scheduleLogReplicationTask());
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setLeaderId(leaderId);

        leader.onReceiveAppendEntriesRpc(this.mockNodeStateContext, new AppendEntriesRpcMessage(rpc, leaderId, null));
        AppendEntriesResult result = (AppendEntriesResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isSuccess());
    }

}
