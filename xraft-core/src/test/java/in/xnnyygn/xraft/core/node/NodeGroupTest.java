package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
import in.xnnyygn.xraft.core.log.sequence.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.MemorySnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeGroupTest {

    @Test
    public void testGetCountOfMajor() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 1, 0,true);
        group.addNode(new NodeEndpoint("C", "localhost", 2335), 1, 0,false);
        Assert.assertEquals(2, group.getCountOfMajor());
    }

    @Test
    public void testUpgrade() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        NodeEndpoint endpoint = new NodeEndpoint("B", "localhost", 2334);
        group.addNode(endpoint, 1, 0,false);
        Assert.assertEquals(1, group.getCountOfMajor());
        group.upgrade(endpoint.getId());
        Assert.assertEquals(2, group.getCountOfMajor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpgradeNodeNotFound() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.upgrade(new NodeId("B"));
    }

    @Test
    public void testDowngrade() {
        NodeEndpoint endpoint = new NodeEndpoint("A", "localhost", 2333);
        NodeGroup group = new NodeGroup(endpoint);
        Assert.assertEquals(1, group.getCountOfMajor());
        group.downgrade(endpoint.getId());
        Assert.assertEquals(0, group.getCountOfMajor());
        Assert.assertTrue(group.getState(endpoint.getId()).isRemoving());
    }

    @Test
    public void testResetReplicationStates() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicationStates(new NodeId("A"), new MemoryLog());
        Assert.assertEquals(1, group.findReplicationState(new NodeId("B")).getNextIndex());
    }

    // (A, self, major, 0), (B, peer, major, 10), (C, peer, major, 0)
    // (A, self, major, 0), (B, peer, major, 10), (C, peer, major, 10)
    @Test
    public void testGetMatchIndexOfMajor() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicationStates(NodeId.of("A"), new MemoryLog()); // 1
        group.findReplicationState(NodeId.of("B")).advance(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findReplicationState(NodeId.of("C")).advance(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 0), (B, peer, major, 10), (C, peer, not major, 0), (D, peer, major, 10)
    @Test
    public void testGetMatchIndexOfMajor2() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        endpoints.add(new NodeEndpoint("D", "localhost", 2336)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.downgrade(NodeId.of("C"));
        group.resetReplicationStates(NodeId.of("A"), new MemoryLog()); // 1
        group.findReplicationState(NodeId.of("B")).advance(10);
        group.findReplicationState(NodeId.of("D")).advance(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 10)
    @Test
    public void testGetMatchIndexOfMajor3() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.resetReplicationStates(NodeId.of("A"), new MemoryLog(
                new MemorySnapshot(10, 1),
                new MemoryEntrySequence(11),
                new EventBus()));
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    @Test
    public void getReplicationTargets() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicationStates(NodeId.of("A"), new MemoryLog());
        Collection<ReplicatingState> replicatingStates = group.getReplicationTargets();
        Assert.assertEquals(2, replicatingStates.size());
        Set<NodeId> nodeIds = replicatingStates.stream().map(ReplicatingState::getNodeId).collect(Collectors.toSet());
        Assert.assertFalse(nodeIds.contains(NodeId.of("A")));
    }

    @Test
    public void testAddNode() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 10, 0,false);
        Assert.assertFalse(group.getState(NodeId.of("B")).isMemberOfMajor());
    }

    @Test
    public void testAddNodeExists() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("A", "localhost", 2333), 10, 0,false);
        Assert.assertFalse(group.getState(NodeId.of("A")).isMemberOfMajor());
    }

    @Test
    public void testRemoveNode() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        NodeId nodeId = NodeId.of("B");
        Assert.assertNotNull(group.getState(nodeId));
        group.removeNode(nodeId);
        Assert.assertNull(group.getState(nodeId));

    }

    @Test
    public void getNodeEndpointsOfMajor() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.addNode(new NodeEndpoint("D", "localhost", 2336), 10, 0,false);
        Assert.assertEquals(3, group.getNodeEndpointsOfMajor().size());
    }

    @Test
    public void testIsUniqueNode() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        Assert.assertTrue(group.isUniqueNode(NodeId.of("A")));
        Assert.assertFalse(group.isUniqueNode(NodeId.of("B")));
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 10, 0,false);
        Assert.assertFalse(group.isUniqueNode(NodeId.of("A")));
    }
}