package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.replication.GeneralReplicationState;
import in.xnnyygn.xraft.core.log.replication.ReplicationStateTracker;
import in.xnnyygn.xraft.core.log.replication.ReplicationState;
import in.xnnyygn.xraft.core.log.replication.GeneralReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneralReplicationStateTrackerTest {

    private ReplicationStateTracker createReplicationStateTrackerWithMatchIndices(List<Integer> matchIndices) {
        Map<NodeId, ReplicationState> map = new HashMap<>();
        for (int i = 0; i < matchIndices.size(); i++) {
            NodeId nodeId = new NodeId(String.valueOf((char) ('A' + i)));
            GeneralReplicationState replicationState = new GeneralReplicationState(nodeId, matchIndices.get(i) + 1);
            replicationState.setMatchIndex(matchIndices.get(i));
            map.put(nodeId, replicationState);
        }
        return new GeneralReplicationStateTracker(map);
    }

    @Test
    public void testGetMajorMatchIndexStart() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 1, 1, 1));
        Assert.assertEquals(1, tracker.getMajorMatchIndex());
    }

    @Test
    public void testGetMajorMatchIndexNodeCount3() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(3, 2, 1));
        Assert.assertEquals(2, tracker.getMajorMatchIndex());
    }

    @Test
    public void testGetMajorMatchIndexNodeCount4() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 2, 3));
        Assert.assertEquals(2, tracker.getMajorMatchIndex());
    }

    @Test
    public void testGetMajorMatchIndexSort() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(3, 2, 1, 3));
        Assert.assertEquals(3, tracker.getMajorMatchIndex());
    }

}