package in.xnnyygn.xraft.core.log;

public class PeerReplicationStateTrackerTest {

//    private ReplicationStateTracker createReplicationStateTrackerWithMatchIndices(List<Integer> matchIndices) {
//        Map<NodeId, ReplicatingState> map = new HashMap<>();
//        for (int i = 0; i < matchIndices.size(); i++) {
//            NodeId nodeId = new NodeId(String.valueOf((char) ('A' + i)));
//            PeerReplicatingState replicationState = new PeerReplicatingState(nodeId, matchIndices.get(i) + 1);
//            replicationState.setMatchIndex(matchIndices.get(i));
//            map.put(nodeId, replicationState);
//        }
//        return new DefaultReplicationStateTracker(map);
//    }
//
//    @Test
//    public void testGetMajorMatchIndexStart() {
//        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 1, 1, 1));
//        Assert.assertEquals(1, tracker.getMajorMatchIndex());
//    }
//
//    @Test
//    public void testGetMajorMatchIndexNodeCount3() {
//        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(3, 2, 1));
//        Assert.assertEquals(2, tracker.getMajorMatchIndex());
//    }
//
//    @Test
//    public void testGetMajorMatchIndexNodeCount4() {
//        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 2, 3));
//        Assert.assertEquals(2, tracker.getMajorMatchIndex());
//    }
//
//    @Test
//    public void testGetMajorMatchIndexSort() {
//        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(3, 2, 1, 3));
//        Assert.assertEquals(3, tracker.getMajorMatchIndex());
//    }

}