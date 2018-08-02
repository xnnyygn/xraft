package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpcMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RequestVoteRpcTest {

    private Log log;
    private MockConnector mockConnector;
    private MockNodeRoleContext mockNodeStateContext;

    @Before
    public void setUp() {
        this.log = new MemoryLog();
        this.mockConnector = new MockConnector();

        this.mockNodeStateContext = new MockNodeRoleContext();
        this.mockNodeStateContext.setSelfNodeId(new NodeId("N"));
        this.mockNodeStateContext.setLog(log);
        this.mockNodeStateContext.setConnector(this.mockConnector);
    }

    @Test
    public void testOnReceiveRequestVoteRpcLowerTerm() {
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(1);
        rpc.setCandidateId(new NodeId("C1"));

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());
        Assert.assertEquals(rpc.getCandidateId(), this.mockConnector.getDestinationNodeId());
    }

    @Test
    public void testOnReceiveRequestVoteRpcHigherTerm() {
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(new NodeId("C1"));

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());
        Assert.assertEquals(rpc.getCandidateId(), this.mockConnector.getDestinationNodeId());
        Assert.assertNotNull(this.mockNodeStateContext.getNodeState().takeSnapshot().getVotedFor());
    }

    @Test
    public void testOnReceiveRequestVoteRpcHigherTermOlderLog() {
        this.log.appendEntry(1, new byte[0]);

        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(new NodeId("C1"));
        rpc.setLastLogTerm(0);
        rpc.setLastLogIndex(0);

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());
        Assert.assertEquals(rpc.getCandidateId(), this.mockConnector.getDestinationNodeId());

        RoleStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(3, snapshot.getTerm());
        Assert.assertNull(snapshot.getVotedFor());
    }

    @Test
    public void testOnReceiveRequestVoteRpcVoted() {
        NodeId candidateId = new NodeId("C1");
        FollowerNodeRole follower = new FollowerNodeRole(2, candidateId, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(2);
        rpc.setCandidateId(candidateId);

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcLogSame() {
        NodeId candidateId = new NodeId("C1");
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(2);
        rpc.setCandidateId(candidateId);

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcLogNewer() {
        this.log.appendEntry(1, new byte[0]);
        this.log.appendEntry(2, new byte[0]);

        NodeId candidateId = new NodeId("C1");
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(2);
        rpc.setCandidateId(candidateId);
        rpc.setLastLogIndex(1);
        rpc.setLastLogTerm(1);

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcLogOlder() {
        this.log.appendEntry(1, new byte[0]);

        NodeId candidateId = new NodeId("C1");
        FollowerNodeRole follower = new FollowerNodeRole(2, null, null, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(2);
        rpc.setCandidateId(candidateId);
        rpc.setLastLogIndex(2);
        rpc.setLastLogTerm(2);

        follower.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcCandidate() {
        CandidateNodeRole candidate = new CandidateNodeRole(2, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(2);
        rpc.setCandidateId(new NodeId("C1"));

        candidate.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(2, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcCandidateToFollower() {
        NodeId candidateId = new NodeId("C1");
        CandidateNodeRole candidate = new CandidateNodeRole(2, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(candidateId);

        candidate.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());

        RoleStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(3, snapshot.getTerm());
        Assert.assertEquals(snapshot.getVotedFor(), candidateId);
    }

    @Test
    public void testOnReceiveRequestVoteRpcCandidateToFollowerNotVoted() {
        this.log.appendEntry(2, new byte[0]);

        NodeId candidateId = new NodeId("C1");
        CandidateNodeRole candidate = new CandidateNodeRole(2, this.mockNodeStateContext.scheduleElectionTimeout());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(candidateId);

        candidate.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));

        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());

        RoleStateSnapshot snapshot = this.mockNodeStateContext.getNodeState().takeSnapshot();
        Assert.assertEquals(3, snapshot.getTerm());
        Assert.assertNull(snapshot.getVotedFor());
    }

    @Test
    public void testOnReceiveRequestVoteRpcLeader() {
        NodeId candidateId = new NodeId("C1");
        LeaderNodeRole leader = new LeaderNodeRole(2, this.mockNodeStateContext.scheduleLogReplicationTask());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(candidateId);
        leader.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));
        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertTrue(result.isVoteGranted());
    }

    @Test
    public void testOnReceiveRequestVoteRpcLeaderNptVoted() {
        this.log.appendEntry(1, new byte[0]);

        NodeId candidateId = new NodeId("C1");
        LeaderNodeRole leader = new LeaderNodeRole(2, this.mockNodeStateContext.scheduleLogReplicationTask());
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(3);
        rpc.setCandidateId(candidateId);
        leader.onReceiveRequestVoteRpc(this.mockNodeStateContext, new RequestVoteRpcMessage(rpc, rpc.getCandidateId(), null));
        RequestVoteResult result = (RequestVoteResult) this.mockConnector.getResult();
        Assert.assertEquals(3, result.getTerm());
        Assert.assertFalse(result.isVoteGranted());
    }
}