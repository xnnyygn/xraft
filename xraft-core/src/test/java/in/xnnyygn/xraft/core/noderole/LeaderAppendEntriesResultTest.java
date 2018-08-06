package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.DefaultLog;
import in.xnnyygn.xraft.core.log.replication.ReplicationState;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.schedule.NullScheduledFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderAppendEntriesResultTest {

    private NodeId nodeId;
    private Log log;
    private MockConnector mockConnector;
    private MockNodeRoleContext mockNodeStateContext;

    @Before
    public void setUp() throws Exception {
        this.nodeId = new NodeId("F1");

        this.log = new DefaultLog();
        this.mockConnector = new MockConnector();

        this.mockNodeStateContext = new MockNodeRoleContext();
        this.mockNodeStateContext.setSelfNodeId(new NodeId("L"));
        this.mockNodeStateContext.setLog(this.log);
        this.mockNodeStateContext.setConnector(this.mockConnector);
    }

    private NodeGroup buildNodeGroup(Collection<NodeId> ids, int nextLogIndex) {
        Set<NodeConfig> configs = ids.stream().map(id -> new NodeConfig(id, new Endpoint("", 0))).collect(Collectors.toSet());
        NodeGroup group = new NodeGroup(configs);
        group.resetReplicationStates(nextLogIndex);
        return group;
    }

    @Test
    public void testOnReceiveAppendEntriesResultHeartbeat() {
        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 1));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext, new AppendEntriesResult("", 1, true), nodeId, new AppendEntriesRpc());

        ReplicationState replicationState = mockNodeStateContext.getNodeGroup().getReplicationState(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultSuccessLog1() {
        this.log.appendEntry(1, new byte[0]); // 1

        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 2));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult("", 1, true),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState = mockNodeStateContext.getNodeGroup().getReplicationState(nodeId);
        Assert.assertEquals(1, replicationState.getMatchIndex());
        Assert.assertEquals(2, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultSuccessLog2() {
        this.log.appendEntry(1, new byte[0]); // 1
        this.log.appendEntry(1, new byte[0]); // 2

        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 2));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult("", 1, true),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState = mockNodeStateContext.getNodeGroup().getReplicationState(nodeId);
        Assert.assertEquals(2, replicationState.getMatchIndex());
        Assert.assertEquals(3, replicationState.getNextIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesFailed() {
        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 2));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext, new AppendEntriesResult("", 1, false), nodeId, new AppendEntriesRpc());

        ReplicationState replicationState = mockNodeStateContext.getNodeGroup().getReplicationState(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
        Assert.assertNotNull(mockConnector.getRpc());
        Assert.assertEquals(this.nodeId, this.mockConnector.getDestinationNodeId());
    }

    @Test
    public void testOnReceiveAppendEntriesResultFailedLog1() {
        this.log.appendEntry(1, new byte[0]); // 1

        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 2));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult("", 1, false),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        ReplicationState replicationState =  mockNodeStateContext.getNodeGroup().getReplicationState(nodeId);
        Assert.assertEquals(0, replicationState.getMatchIndex());
        Assert.assertEquals(1, replicationState.getNextIndex());
        Assert.assertNotNull(this.mockConnector.getRpc());
    }

    @Test
    public void testOnReceiveAppendEntriesResultFailedHigherTerm() {
        this.log.appendEntry(1, new byte[0]); // 1

        mockNodeStateContext.setNodeGroup(buildNodeGroup(Arrays.asList(nodeId, new NodeId("F2")), 2));
        LeaderNodeRole leader = new LeaderNodeRole(1, new LogReplicationTask(new NullScheduledFuture()));

        leader.onReceiveAppendEntriesResult(mockNodeStateContext,
                new AppendEntriesResult("", 2, false),
                nodeId,
                this.log.createAppendEntriesRpc(1, nodeId, 2, -1));

        AbstractNodeRole nodeState = this.mockNodeStateContext.getNodeState();
        Assert.assertEquals(RoleName.FOLLOWER, nodeState.getRole());
        Assert.assertEquals(2, nodeState.getTerm());
    }

}