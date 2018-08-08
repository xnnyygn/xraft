package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.support.ByteArraySeekableFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FileNodeStoreTest {

    @Test
    public void test() throws IOException {
        ByteArraySeekableFile file = new ByteArraySeekableFile();
        FileNodeStore store = new FileNodeStore(file);
        store.initialize();
        Assert.assertEquals(0, store.getCurrentTerm());
        Assert.assertNull(store.getVotedFor());
        Assert.assertEquals(8, file.size());

        store.setCurrentTerm(1);
        NodeId nodeId = new NodeId("A");
        store.setVotedFor(nodeId);

        // (term, 4) + (votedFor length, 4) + (votedFor data, 1) = 9
        Assert.assertEquals(9, file.size());
        file.seek(0);
        Assert.assertEquals(1, file.readInt());
        Assert.assertEquals(1, file.readInt());
        byte[] data = new byte[1];
        file.read(data);
        Assert.assertArrayEquals(nodeId.getValue().getBytes(), data);

        Assert.assertEquals(1, store.getCurrentTerm());
        Assert.assertEquals(nodeId, store.getVotedFor());
    }

}