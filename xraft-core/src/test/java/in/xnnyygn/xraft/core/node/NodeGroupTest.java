package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.log.sequence.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.MemorySnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeGroupTest {

    @Test
    public void testFindMember() {
        NodeEndpoint endpoint = new NodeEndpoint("A", "localhost", 2333);
        NodeGroup group = new NodeGroup(endpoint);
        Assert.assertNotNull(group.findMember(NodeId.of("A")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindMemberNotFound() {
        NodeEndpoint endpoint = new NodeEndpoint("A", "localhost", 2333);
        NodeGroup group = new NodeGroup(endpoint);
        Assert.assertNotNull(group.findMember(NodeId.of("B")));
    }

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
        Assert.assertTrue(group.findMember(endpoint.getId()).isRemoving());
    }

    @Test
    public void testResetReplicatingStates() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog());
        Assert.assertTrue(group.findMember(NodeId.of("A")).isReplicationStateSet());
        Assert.assertTrue(group.findMember(NodeId.of("B")).isReplicationStateSet());
    }

    // no self node
    @Test
    public void testResetReplicatingStates2() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicatingStates(NodeId.of("C"), new MemoryLog());
        Assert.assertTrue(group.findMember(NodeId.of("A")).isReplicationStateSet());
        Assert.assertTrue(group.findMember(NodeId.of("B")).isReplicationStateSet());
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
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog()); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("C")).advanceReplicatingState(10);
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
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog()); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        group.findMember(NodeId.of("D")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 10)
    @Test
    public void testGetMatchIndexOfMajor3() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog(
                new MemorySnapshot(10, 1),
                new MemoryEntrySequence(11),
                new EventBus()));
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 10), (B, peer, major, 9)
    @Test
    public void testGetMatchIndexOfMajor4() {
        NodeGroup group = new NodeGroup(Arrays.asList(
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334)
        ));
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog(
                new MemorySnapshot(10, 1),
                new MemoryEntrySequence(11),
                new EventBus()));
        group.findMember(NodeId.of("B")).advanceReplicatingState(9);
        Assert.assertEquals(9, group.getMatchIndexOfMajor());
    }

    @Test
    public void testListReplicationTarget() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.resetReplicatingStates(NodeId.of("A"), new MemoryLog());
        Collection<GroupMember> replicatingStates = group.listReplicationTarget();
        Assert.assertEquals(2, replicatingStates.size());
        Set<NodeId> nodeIds = replicatingStates.stream().map(GroupMember::getId).collect(Collectors.toSet());
        Assert.assertFalse(nodeIds.contains(NodeId.of("A")));
    }

    @Test
    public void testAddNode() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 10, 0,false);
        Assert.assertFalse(group.findMember(NodeId.of("B")).isMajor());
    }

    @Test
    public void testAddNodeExists() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("A", "localhost", 2333), 10, 0,false);
        Assert.assertFalse(group.findMember(NodeId.of("A")).isMajor());
    }

    @Test
    public void testRemoveNode() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        NodeId nodeId = NodeId.of("B");
        Assert.assertNotNull(group.getMember(nodeId));
        group.removeNode(nodeId);
        Assert.assertNull(group.getMember(nodeId));
    }

    @Test
    public void testUpdateNodes() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);

        Set<NodeEndpoint> endpoints2 = new HashSet<>();
        endpoints2.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints2.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        group.updateNodes(endpoints2);
        Assert.assertEquals(2, group.getCountOfMajor());
    }

    @Test
    public void testListEndpointOfMajor() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        group.addNode(new NodeEndpoint("D", "localhost", 2336), 10, 0,false);
        Assert.assertEquals(3, group.listEndpointOfMajor().size());
    }

    @Test
    public void testListEndpointOfMajorExcept() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints);
        Assert.assertEquals(2, group.listEndpointOfMajorExcept(NodeId.of("A")).size());
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