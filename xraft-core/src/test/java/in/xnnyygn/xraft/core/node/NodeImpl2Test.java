package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.TaskReference;
import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryCommittedEvent;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
import in.xnnyygn.xraft.core.noderole.RoleName;
import in.xnnyygn.xraft.core.noderole.RoleState;
import in.xnnyygn.xraft.core.rpc.MockConnector;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.NullScheduler;
import in.xnnyygn.xraft.core.support.DirectTaskExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeImpl2Test {

    private NodeBuilder2 newNodeBuilder(NodeId selfId, NodeEndpoint... endpoints) {
        return new NodeBuilder2(selfId, new NodeGroup(Arrays.asList(endpoints)))
                .setScheduler(new NullScheduler())
                .setConnector(new MockConnector())
                .setTaskExecutor(new DirectTaskExecutor(true));
    }

    private AppendEntriesRpc createAppendEntriesRpc(int lastEntryIndex) {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(lastEntryIndex);
        return rpc;
    }

    @Test
    public void testStartFresh() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .build();

        node.start();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(0, state.getTerm());
        Assert.assertNull(state.getVotedFor());
    }

    @Test
    public void testStartLoadFromStore() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .setStore(new MemoryNodeStore(1, NodeId.of("B")))
                .build();

        node.start();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(NodeId.of("B"), state.getVotedFor());
    }

    @Test
    public void testElectionTimeoutStandalone() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .build();
        node.start();

        node.electionTimeout();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.LEADER, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());

        // replication state set
        Assert.assertNotNull(node.getContext().group().findReplicationState(NodeId.of("A")));

        // no-op log
        EntryMeta lastEntryMeta = node.getContext().log().getLastEntryMeta();
        Assert.assertEquals(Entry.KIND_NO_OP, lastEntryMeta.getKind());
        Assert.assertEquals(1, lastEntryMeta.getIndex());
        Assert.assertEquals(1, lastEntryMeta.getTerm());
    }

    @Test
    public void testElectionTimeoutStandby() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .setStore(new MemoryNodeStore(1, null))
                .setStandby(true)
                .build();
        node.start();
        node.electionTimeout();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
        // timeout was cancelled
    }

    @Test
    public void testElectionTimeoutWhenLeader() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .build();
        node.start();
        node.electionTimeout();
        node.electionTimeout(); // do nothing
    }

    @Test
    public void testElectionTimeoutWhenFollower() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());
        Assert.assertEquals(1, state.getVotesCount());

        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        RequestVoteRpc rpc = (RequestVoteRpc) mockConnector.getRpc();
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(NodeId.of("A"), rpc.getCandidateId());
        Assert.assertEquals(0, rpc.getLastLogIndex());
        Assert.assertEquals(0, rpc.getLastLogTerm());
    }

    @Test
    public void testElectionTimeoutWhenCandidate() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // become candidate
        node.electionTimeout();

        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertEquals(2, state.getTerm());
        Assert.assertEquals(1, state.getVotesCount());

        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        RequestVoteRpc rpc = (RequestVoteRpc) mockConnector.getLastMessage().getRpc();
        Assert.assertEquals(2, rpc.getTerm());
        Assert.assertEquals(NodeId.of("A"), rpc.getCandidateId());
        Assert.assertEquals(0, rpc.getLastLogIndex());
        Assert.assertEquals(0, rpc.getLastLogTerm());
    }

    @Test
    public void testReplicateLogStandalone() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .build();
        node.start();
        node.electionTimeout();
        Assert.assertEquals(0, node.getContext().log().getCommitIndex());
        node.replicateLog();
        Assert.assertEquals(1, node.getContext().log().getCommitIndex());
    }

    @Test
    public void testReplicateLog() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // request vote rpc
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        node.replicateLog(); // append entries * 2

        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        Assert.assertEquals(3, mockConnector.getMessageCount());

        // check destination node id
        List<MockConnector.Message> messages = mockConnector.getMessages();
        Set<NodeId> destinationNodeIds = messages.subList(1, 3).stream()
                .map(MockConnector.Message::getDestinationNodeId)
                .collect(Collectors.toSet());
        Assert.assertEquals(2, destinationNodeIds.size());
        Assert.assertTrue(destinationNodeIds.contains(NodeId.of("B")));
        Assert.assertTrue(destinationNodeIds.contains(NodeId.of("C")));

        AppendEntriesRpc rpc = (AppendEntriesRpc) messages.get(2).getRpc();
        Assert.assertEquals(1, rpc.getTerm());
    }

    @Test
    public void testReplicateLogSkipReplicating() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        node.getContext().group().findReplicationState(NodeId.of("B")).startReplicating();
        node.replicateLog();

        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        // request vote rpc + append entries rpc
        Assert.assertEquals(2, mockConnector.getMessageCount());
    }

    @Test
    public void testReplicateLogForceReplicating() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        long replicatedAt = System.currentTimeMillis() - node.getContext().config().getMinReplicationInterval() - 1;
        node.getContext().group().findReplicationState(NodeId.of("B")).startReplicating(replicatedAt);
        node.replicateLog();

        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        // request vote rpc + append entries rpc * 2
        Assert.assertEquals(3, mockConnector.getMessageCount());
    }

    @Test(expected = NotLeaderException.class)
    public void testAppendLogWhenFollower() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.appendLog("test".getBytes());
    }

    @Test(expected = NotLeaderException.class)
    public void testAppendLogWhenCandidate() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();
        node.appendLog("test".getBytes());
    }


    @Test
    public void testAppendLog() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        node.appendLog("test".getBytes());
        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        // request vote rpc + append entries * 2
        Assert.assertEquals(3, mockConnector.getMessageCount());
    }


    @Test(expected = NotLeaderException.class)
    public void testAddServerWhenFollower() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.addServer(new NodeEndpoint("D", "localhost", 2336));
    }

    @Test(expected = NotLeaderException.class)
    public void testAddServerWhenCandidate() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();
        node.addServer(new NodeEndpoint("D", "localhost", 2336));
    }

    @Test
    public void testAddServer() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        MockConnector connector = (MockConnector) node.getContext().connector();
        node.start();
        node.electionTimeout();
        connector.clearMessage();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));

        TaskReference reference = node.addServer(new NodeEndpoint("D", "localhost", 2336));
        Assert.assertEquals(1, connector.getMessageCount());
        connector.clearMessage();

        // catch up
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(1)));

        node.replicateLog();
        Assert.assertEquals(3, connector.getMessageCount()); // B, C, D
        connector.clearMessage();

        GroupConfigEntry groupConfigEntry = node.getContext().log().getLastUncommittedGroupConfigEntry();

        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("B"), createAppendEntriesRpc(2)));

        // commit new node log
        node.onGroupConfigEntryCommitted(new GroupConfigEntryCommittedEvent(groupConfigEntry));
        Assert.assertEquals(TaskReference.Result.OK, reference.getResult());
    }

    @Test
    public void testAddServerCannotCatchUp() {
        NodeConfig2 config = new NodeConfig2();
        config.setNewNodeMaxRound(1);

        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .setConfig(config)
                .build();
        MockConnector connector = (MockConnector) node.getContext().connector();
        node.start();
        node.electionTimeout();
        connector.clearMessage();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));

        TaskReference reference = node.addServer(new NodeEndpoint("D", "localhost", 2336));
        Assert.assertEquals(1, connector.getMessageCount());
        connector.clearMessage();

        // cannot catch up
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(0)));

        Assert.assertEquals(TaskReference.Result.TIMEOUT, reference.getResult());
    }

    @Test
    public void testAddServerAwaitPreviousGroupConfigChange() {
        NodeConfig2 config = new NodeConfig2();
        config.setPreviousGroupConfigChangeTimeout(1);
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .setConfig(config)
                .build();
        MockConnector connector = (MockConnector) node.getContext().connector();
        node.start();
        node.electionTimeout();
        connector.clearMessage();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));

        node.addServer(new NodeEndpoint("D", "localhost", 2336));
        System.out.println(connector.getMessages());
        connector.clearMessage();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(1)));

        TaskReference reference = node.addServer(new NodeEndpoint("E", "localhost", 2337));
        Assert.assertEquals(TaskReference.Result.TIMEOUT, reference.getResult());
    }

    //    @Test
//    public void removeServer() {
//    }
//
//    @Test
//    public void onReceiveRequestVoteRpc() {
//    }
//
    @Test
    public void testOnReceiveRequestVoteResult() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();

        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));

        // role state
        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.LEADER, state.getRoleName());
        Assert.assertEquals(1, state.getTerm());

        // replication state set
        Assert.assertNotNull(node.getContext().group().findReplicationState(NodeId.of("A")));
        Assert.assertNotNull(node.getContext().group().findReplicationState(NodeId.of("B")));
        Assert.assertNotNull(node.getContext().group().findReplicationState(NodeId.of("C")));

        // no-op log
        EntryMeta lastEntryMeta = node.getContext().log().getLastEntryMeta();
        Assert.assertEquals(Entry.KIND_NO_OP, lastEntryMeta.getKind());
        Assert.assertEquals(1, lastEntryMeta.getIndex());
        Assert.assertEquals(1, lastEntryMeta.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultNotGranted() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();

        node.onReceiveRequestVoteResult(new RequestVoteResult(1, false)); // do nothing
    }

    @Test
    public void testOnReceiveRequestVoteResultLargerTerm() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // become candidate

        node.onReceiveRequestVoteResult(new RequestVoteResult(2, false));
        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(2, state.getTerm());
    }

    @Test
    public void testOnReceiveRequestVoteResultWhenLeader() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .build();
        node.start();
        node.electionTimeout(); // become leader

        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // do nothing
    }

    @Test
    public void testOnReceiveRequestVoteResultWhenFollower() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333))
                .setStore(new MemoryNodeStore(1, null))
                .build();
        node.start();

        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
    }

    @Test
    public void testOnReceiveRequestVoteResultLessThanHalfOfMajorCount() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2333),
                new NodeEndpoint("C", "localhost", 2333),
                new NodeEndpoint("D", "localhost", 2333),
                new NodeEndpoint("E", "localhost", 2333)
        ).setStore(new MemoryNodeStore(1, null))
                .build();
        node.start();
        node.electionTimeout();

        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.CANDIDATE, state.getRoleName());
        Assert.assertEquals(2, state.getVotesCount());
    }

    //
//    @Test
//    public void onReceiveAppendEntriesRpc() {
//    }
//
    @Test
    public void testOnReceiveAppendEntriesResultPeerCatchUp() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        ReplicatingState replicatingState = node.getContext().group().findReplicationState(NodeId.of("B"));
        replicatingState.startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("B"), createAppendEntriesRpc(1)));
        Assert.assertFalse(replicatingState.isReplicating());
        Assert.assertEquals(1, replicatingState.getMatchIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultPeerNotCatchUp() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        ReplicatingState replicatingState = node.getContext().group().findReplicationState(NodeId.of("B"));
        replicatingState.startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("B"), createAppendEntriesRpc(0)));
        Assert.assertTrue(replicatingState.isReplicating());
        Assert.assertEquals(0, replicatingState.getMatchIndex());
        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        Assert.assertEquals(2, mockConnector.getMessageCount());
        MockConnector.Message message = mockConnector.getLastMessage();
        Assert.assertTrue(message.getRpc() instanceof AppendEntriesRpc);
        Assert.assertEquals(NodeId.of("B"), message.getDestinationNodeId());
    }

    @Test
    public void testOnReceiveAppendEntriesResultBackOff() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .setStore(new MemoryNodeStore(1, null))
                .build();
        node.getContext().log().appendEntry(1);
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(2, true)); // become leader
        ReplicatingState replicatingState = node.getContext().group().findReplicationState(NodeId.of("B"));
        replicatingState.startReplicating();
        Assert.assertEquals(2, replicatingState.getNextIndex());
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, false),
                NodeId.of("B"), createAppendEntriesRpc(1)));
        Assert.assertTrue(replicatingState.isReplicating());
        Assert.assertEquals(1, replicatingState.getNextIndex());
        Assert.assertEquals(0, replicatingState.getMatchIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultBackOffFailed() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        ReplicatingState replicatingState = node.getContext().group().findReplicationState(NodeId.of("B"));
        replicatingState.startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, false),
                NodeId.of("B"), createAppendEntriesRpc(1)));
        Assert.assertFalse(replicatingState.isReplicating());
        Assert.assertEquals(0, replicatingState.getMatchIndex());
    }

    @Test
    public void testOnReceiveAppendEntriesResultLargerTerm() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 3, false),
                NodeId.of("B"), createAppendEntriesRpc(1)));
        RoleState state = node.getRoleState2();
        Assert.assertEquals(RoleName.FOLLOWER, state.getRoleName());
        Assert.assertEquals(3, state.getTerm());
    }

    @Test
    public void testOnReceiveAppendEntriesResultUnexpectedSourceNodeId() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, false),
                NodeId.of("D"), createAppendEntriesRpc(1)));
    }

    @Test
    public void testOnReceiveAppendEntriesResultNodeRemoving() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        NodeGroup.NodeState nodeState = node.getContext().group().addNode(new NodeEndpoint("D", "localhost", 2336), 2, false);
        nodeState.getReplicatingState().startReplicating();
        nodeState.setRemoving(true);
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(1)));
        Assert.assertFalse(nodeState.getReplicatingState().isReplicating());
    }

    @Test
    public void testOnReceiveAppendEntriesResultNewNodeCatchUp() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        NodeGroup.NodeState nodeState = node.getContext().group().addNode(new NodeEndpoint("D", "localhost", 2336), 2, false);
        nodeState.getReplicatingState().startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(1)));
        Assert.assertTrue(nodeState.isMemberOfMajor());
        Assert.assertFalse(nodeState.getReplicatingState().isReplicating());

        EntryMeta lastEntryMeta = node.getContext().log().getLastEntryMeta();
        Assert.assertEquals(Entry.KIND_ADD_NODE, lastEntryMeta.getKind());
        Assert.assertEquals(2, lastEntryMeta.getIndex());
        Assert.assertEquals(1, lastEntryMeta.getTerm());
    }

    @Test
    public void testOnReceiveAppendEntriesResultNewNodeCannotCatchUp() {
        NodeConfig2 config = new NodeConfig2();
        config.setNewNodeMaxRound(1);
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .setConfig(config)
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        NodeGroup.NodeState nodeState = node.getContext().group().addNode(new NodeEndpoint("D", "localhost", 2336), 2, false);
        nodeState.getReplicatingState().startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(0)));
        Assert.assertNull(node.getContext().group().getState(NodeId.of("D")));
    }

    @Test
    public void testOnReceiveAppendEntriesResultNewNodeContinue() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        NodeGroup.NodeState nodeState = node.getContext().group().addNode(new NodeEndpoint("D", "localhost", 2336), 2, false);
        nodeState.getReplicatingState().startReplicating();
        node.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"), createAppendEntriesRpc(0)));
        Assert.assertFalse(nodeState.isMemberOfMajor());
        Assert.assertTrue(nodeState.getReplicatingState().isReplicating());
        MockConnector mockConnector = (MockConnector) node.getContext().connector();
        // request vote rpc + append entries rpc
        Assert.assertEquals(2, mockConnector.getMessageCount());
    }

    //    @Test
//    public void onReceiveInstallSnapshotRpc() {
//    }
//
//    @Test
//    public void onReceiveInstallSnapshotResult() {
//    }
//
//    @Test
//    public void onGroupConfigEntryFromLeaderAppend() {
//    }
//
    @Test
    public void testOnGroupConfigEntryCommittedAddNode() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        AddNodeEntry groupConfigEntry = new AddNodeEntry(2, 1, Collections.emptySet(), new NodeEndpoint("D", "localhost", 2336));
        node.onGroupConfigEntryCommitted(new GroupConfigEntryCommittedEvent(groupConfigEntry));
    }

    @Test
    public void testOnGroupConfigEntryCommittedRemoveNode() {
        NodeImpl2 node = (NodeImpl2) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335))
                .build();
        node.start();
        node.electionTimeout(); // become candidate
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true)); // become leader
        // TODO here
    }
//
//    @Test
//    public void onGroupConfigEntryBatchRemoved() {
//    }
}