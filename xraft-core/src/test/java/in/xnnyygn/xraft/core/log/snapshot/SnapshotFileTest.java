package in.xnnyygn.xraft.core.log.snapshot;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class SnapshotFileTest {

    @Test
    @Ignore
    public void testWrite() {
        SnapshotFile.create(new MemorySnapshot(1, 1, "Hello, world!".getBytes()), "kvstore.ss");
    }

    @Test
    @Ignore
    public void testRead() {
        SnapshotFile file = new SnapshotFile("kvstore.ss");
        Assert.assertEquals(1, file.getLastIncludedIndex());
        Assert.assertEquals(1, file.getLastIncludedTerm());
        Assert.assertEquals(13, file.size());
        System.out.println(new String(file.toByteArray()));
    }

    @Test
    public void testReplace() {
        SnapshotFile file = new SnapshotFile("kvstore.ss");
        file.replace(new MemorySnapshot(2, 2, "Goodbye, world!".getBytes()));
        Assert.assertEquals(2, file.getLastIncludedIndex());
        Assert.assertEquals(2, file.getLastIncludedTerm());
        Assert.assertEquals("Goodbye, world!", new String(file.toByteArray()));
    }

}