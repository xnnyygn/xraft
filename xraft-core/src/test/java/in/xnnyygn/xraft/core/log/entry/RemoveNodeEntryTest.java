package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class RemoveNodeEntryTest {

    @Test
    public void getResultNodeEndpoints() {
        RemoveNodeEntry entry = new RemoveNodeEntry(1, 1,
                Collections.singleton(new NodeEndpoint("A", "localhost", 2333)), NodeId.of("A"));
        Assert.assertTrue(entry.getResultNodeEndpoints().isEmpty());
    }

}