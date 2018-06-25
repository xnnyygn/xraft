package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ReplicationStateTrackerTest {

    private ReplicationStateTracker createReplicationStateTrackerWithMatchIndices(List<Integer> matchIndices) {
        List<NodeId> nodeIds = IntStream.range(0, matchIndices.size()).boxed().map(i -> new NodeId(String.valueOf(i))).collect(Collectors.toList());
        ReplicationStateTracker tracker = new ReplicationStateTracker(nodeIds, 1);
        for (int i = 0; i < matchIndices.size(); i++) {
            tracker.get(nodeIds.get(i)).setMatchIndex(matchIndices.get(i));
        }
        return tracker;
    }

    @Test
    public void testGetMajorMatchIndexStart() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 1, 1, 1));
        Assert.assertEquals(1, tracker.getMajorMatchIndex());
    }

    @Test
    public void testGetMajorMatchIndexNodeCount5() {
        ReplicationStateTracker tracker = createReplicationStateTrackerWithMatchIndices(Arrays.asList(1, 2, 3, 4));
        Assert.assertEquals(3, tracker.getMajorMatchIndex());
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