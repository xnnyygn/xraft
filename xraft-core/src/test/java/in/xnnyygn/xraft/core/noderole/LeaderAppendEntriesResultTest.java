package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.*;
import in.xnnyygn.xraft.core.log.replication.ReplicationState;
import in.xnnyygn.xraft.core.log.replication.GeneralReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.schedule.NullScheduledFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class LeaderAppendEntriesResultTest {

    private NodeId nodeId;
    private Log log;
    private MockConnector mockConnector;
    private MockNodeRoleContext mockNodeStateContext;

    @Before
    public void setUp() throws Exception {
        this.nodeId = new NodeId("F1");

        this.log = new MemoryLog();
        this.mockConnector = new MockConnector();

        this.mockNodeStateContext = new MockNodeRoleContext();
        this.mockNodeStateContext.setSelfNodeId(new NodeId("L"));
        this.mockNodeStateContext.setLog(this.log);
        this.mockNodeStateContext.setConnector(this.mockConnector);
    }

    @Test
    public void testOnReceiveAppendEntriesResultHeartbeat() {
        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 1);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext, new AppendEntriesResult(1, true), nodeId, new AppendEntriesRpc());

        ReplicationState replicationState = tracker.get(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultSuccessLog1() {
        this.log.appendEntry(1, new byte[0]); // 1

        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 2);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult(1, true),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState = tracker.get(nodeId);
        Assert.assertEquals(1, replicationState.getMatchIndex());
        Assert.assertEquals(2, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultSuccessLog2() {
        this.log.appendEntry(1, new byte[0]); // 1
        this.log.appendEntry(1, new byte[0]); // 2

        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 2);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult(1, true),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState = tracker.get(nodeId);
        Assert.assertEquals(2, replicationState.getMatchIndex());
        Assert.assertEquals(3, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesFailed() {
        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 1);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext, new AppendEntriesResult(1, false), nodeId, new AppendEntriesRpc());

        ReplicationState replicationState = tracker.get(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
        Assert.assertNotNull(this.mockConnector.getRpc());
        Assert.assertEquals(this.nodeId, this.mockConnector.getDestinationNodeId());
    }

    @Test
    public void testOnReceiveAppendEntriesResultFailedLog1() {
        this.log.appendEntry(1, new byte[0]); // 1

        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 2);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult(1, false),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState = tracker.get(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
        Assert.assertNotNull(this.mockConnector.getRpc());
    }

    @Test
    public void testOnReceiveAppendEntriesResultFailedHigherTerm() {
        this.log.appendEntry(1, new byte[0]); // 1

        GeneralReplicationStateTracker tracker = new GeneralReplicationStateTracker(Arrays.asList(nodeId, new NodeId("F2")), 2);
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()), tracker);

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult(2, false),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        AbstractNodeRole nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(RoleName.FOLLOWER, nodeState.getRole());
        Assert.assertEquals(2, nodeState.getTerm());
    }

}