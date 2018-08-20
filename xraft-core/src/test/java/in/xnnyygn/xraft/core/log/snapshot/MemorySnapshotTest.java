package in.xnnyygn.xraft.core.log.snapshot;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MemorySnapshotTest {

    @Test
    public void testReadEmpty() {
        MemorySnapshot snapshot = new MemorySnapshot(0, 0);
        snapshot.readData(0, 10);
    }

    @Test
    public void testRead1() {
        MemorySnapshot snapshot = new MemorySnapshot(0, 0, "foo".getBytes(), Collections.emptySet());
        SnapshotChunk chunk1 = snapshot.readData(0, 2);
        Assert.assertArrayEquals(new byte[]{'f', 'o'}, chunk1.toByteArray());
        Assert.assertFalse(chunk1.isLastChunk());
        SnapshotChunk lastChunk = snapshot.readData(2, 2);
        Assert.assertArrayEquals(new byte[]{'o'}, lastChunk.toByteArray());
        Assert.assertTrue(lastChunk.isLastChunk());
    }

    @Test
    public void testRead2() {
        MemorySnapshot snapshot = new MemorySnapshot(0, 0, "foo,".getBytes(), Collections.emptySet());
        Assert.assertArrayEquals(new byte[]{'f', 'o'}, snapshot.readData(0, 2).toByteArray());
        Assert.assertArrayEquals(new byte[]{'o', ','}, snapshot.readData(2, 2).toByteArray());
    }

}