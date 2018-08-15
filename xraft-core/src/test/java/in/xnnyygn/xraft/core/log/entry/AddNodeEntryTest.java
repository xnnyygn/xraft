package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.*;

public class AddNodeEntryTest {

    @Test
    public void testGetResultNodeEndpoints() {
        AddNodeEntry entry = new AddNodeEntry(1, 1, Collections.emptySet(),
                new NodeEndpoint("A", "localhost", 2333));
        Set<NodeEndpoint> nodeEndpoints = entry.getResultNodeEndpoints();
        Assert.assertEquals(1, nodeEndpoints.size());
        Assert.assertEquals(NodeId.of("A"), nodeEndpoints.iterator().next().getId());
    }

}