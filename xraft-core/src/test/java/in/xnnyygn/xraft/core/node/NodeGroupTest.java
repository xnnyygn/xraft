package in.xnnyygn.xraft.core.node;

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
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 1, 0, true);
        group.addNode(new NodeEndpoint("C", "localhost", 2335), 1, 0, false);
        Assert.assertEquals(2, group.getCountOfMajor());
    }

    @Test
    public void testUpgrade() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        NodeEndpoint endpoint = new NodeEndpoint("B", "localhost", 2334);
        group.addNode(endpoint, 1, 0, false);
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
    public void testResetReplicatingStates() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1);
        Assert.assertFalse(group.findMember(NodeId.of("A")).isReplicationStateSet());
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
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("C")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 0), (B, peer, major, 10), (C, peer, not major, 0), (D, peer, major, 10)
//    @Test
//    public void testGetMatchIndexOfMajor2() {
//        Set<NodeEndpoint> endpoints = new HashSet<>();
//        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
//        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
//        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
//        endpoints.add(new NodeEndpoint("D", "localhost", 2336)); // peer
//        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
//        group.downgrade(NodeId.of("C"));
//        group.resetReplicatingStates(1); // 1
//        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
//        group.findMember(NodeId.of("D")).advanceReplicatingState(10);
//        Assert.assertEquals(10, group.getMatchIndexOfMajor());
//    }

    // standalone
    @Test(expected = IllegalStateException.class)
    public void testGetMatchIndexOfMajor3() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.resetReplicatingStates(11);
        group.getMatchIndexOfMajor();
    }

    // (A, self, major, 10), (B, peer, major, 9)
    @Test
    public void testGetMatchIndexOfMajor4() {
        NodeGroup group = new NodeGroup(Arrays.asList(
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334)
        ), NodeId.of("A"));
        group.resetReplicatingStates(11);
        group.findMember(NodeId.of("B")).advanceReplicatingState(9);
        Assert.assertEquals(9, group.getMatchIndexOfMajor());
    }

    // (A, self, major, 10), (B, peer, major, 10), (C, peer, major, 0), (D, peer, major, 0)
    @Test
    public void testGetMatchIndexOfMajor5() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        endpoints.add(new NodeEndpoint("D", "localhost", 2336)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("C")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    @Test
    public void testGetMatchIndexOfMajor6() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        endpoints.add(new NodeEndpoint("D", "localhost", 2336)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("C")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    @Test
    public void testGetMatchIndexOfMajor7() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        endpoints.add(new NodeEndpoint("D", "localhost", 2336)); // peer
        endpoints.add(new NodeEndpoint("E", "localhost", 2336)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1); // 1
        group.findMember(NodeId.of("B")).advanceReplicatingState(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("C")).advanceReplicatingState(10);
        Assert.assertEquals(0, group.getMatchIndexOfMajor());
        group.findMember(NodeId.of("D")).advanceReplicatingState(10);
        Assert.assertEquals(10, group.getMatchIndexOfMajor());
    }

    @Test
    public void testListReplicationTarget() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.resetReplicatingStates(1);
        Collection<GroupMember> replicatingStates = group.listReplicationTarget();
        Assert.assertEquals(2, replicatingStates.size());
        Set<NodeId> nodeIds = replicatingStates.stream().map(GroupMember::getId).collect(Collectors.toSet());
        Assert.assertFalse(nodeIds.contains(NodeId.of("A")));
    }

    @Test
    public void testAddNode() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 10, 0, false);
        Assert.assertFalse(group.findMember(NodeId.of("B")).isMajor());
    }

    @Test
    public void testAddNodeExists() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        group.addNode(new NodeEndpoint("A", "localhost", 2333), 10, 0, false);
        Assert.assertFalse(group.findMember(NodeId.of("A")).isMajor());
    }

    @Test
    public void testRemoveNode() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
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
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));

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
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        group.addNode(new NodeEndpoint("D", "localhost", 2336), 10, 0, false);
        Assert.assertEquals(3, group.listEndpointOfMajor().size());
    }

    @Test
    public void testListEndpointOfMajorExceptSelf() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        endpoints.add(new NodeEndpoint("A", "localhost", 2333)); // self
        endpoints.add(new NodeEndpoint("B", "localhost", 2334)); // peer
        endpoints.add(new NodeEndpoint("C", "localhost", 2335)); // peer
        NodeGroup group = new NodeGroup(endpoints, NodeId.of("A"));
        Assert.assertEquals(2, group.listEndpointOfMajorExceptSelf().size());
    }

    @Test
    public void testIsStandalone() {
        NodeGroup group = new NodeGroup(new NodeEndpoint("A", "localhost", 2333));
        Assert.assertTrue(group.isStandalone());
        group.addNode(new NodeEndpoint("B", "localhost", 2334), 10, 0, false);
        Assert.assertFalse(group.isStandalone());
    }
}