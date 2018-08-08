package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.support.ByteArraySeekableFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FileSnapshotTest {

    @Test
    public void test() throws IOException {
        ByteArraySeekableFile file = new ByteArraySeekableFile();
        file.writeInt(1);
        file.writeInt(2);
        byte[] data = "test".getBytes();
        file.write(data);
        file.seek(0);

        FileSnapshot snapshot = new FileSnapshot(file);
        Assert.assertEquals(1, snapshot.getLastIncludedIndex());
        Assert.assertEquals(2, snapshot.getLastIncludedTerm());
        Assert.assertEquals(4, snapshot.getDataSize());
        SnapshotChunk chunk = snapshot.readData(0, 10);
        Assert.assertArrayEquals(data, chunk.toByteArray());
        Assert.assertTrue(chunk.isLastChunk());
    }

}