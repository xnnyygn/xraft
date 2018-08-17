package in.xnnyygn.xraft.core.node.store;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.store.FileNodeStore;
import in.xnnyygn.xraft.core.support.ByteArraySeekableFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FileNodeStoreTest {

    // 1, A
    @Test
    public void testRead() throws IOException {
        ByteArraySeekableFile file = new ByteArraySeekableFile();
        file.writeInt(1);
        file.writeInt(1);
        file.write("A".getBytes());
        file.seek(0L);
        FileNodeStore store = new FileNodeStore(file);
        Assert.assertEquals(1, store.getTerm());
        Assert.assertEquals(NodeId.of("A"), store.getVotedFor());
    }

    // 1, null
    @Test
    public void testRead2() throws IOException {
        ByteArraySeekableFile file = new ByteArraySeekableFile();
        file.writeInt(1);
        file.writeInt(0);
        file.seek(0L);
        FileNodeStore store = new FileNodeStore(file);
        Assert.assertEquals(1, store.getTerm());
        Assert.assertNull(store.getVotedFor());
    }

    @Test
    public void testWrite() throws IOException {
        ByteArraySeekableFile file = new ByteArraySeekableFile();
        FileNodeStore store = new FileNodeStore(file);
        Assert.assertEquals(0, store.getTerm());
        Assert.assertNull(store.getVotedFor());
        Assert.assertEquals(8, file.size());

        store.setTerm(1);
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

        Assert.assertEquals(1, store.getTerm());
        Assert.assertEquals(nodeId, store.getVotedFor());
    }

}