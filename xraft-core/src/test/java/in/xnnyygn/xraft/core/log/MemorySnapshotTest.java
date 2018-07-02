package in.xnnyygn.xraft.core.log;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemorySnapshotTest {

    @Test
    public void testRead1() {
        MemorySnapshot snapshot = new MemorySnapshot("Hello".getBytes());
        byte[] buffer = new byte[2];
        Assert.assertEquals(2, snapshot.read(buffer));
        Assert.assertArrayEquals(new byte[]{'H', 'e'}, buffer);
        Assert.assertEquals(2, snapshot.read(buffer));
        Assert.assertArrayEquals(new byte[]{'l', 'l'}, buffer);
        Assert.assertEquals(1, snapshot.read(buffer));
        Assert.assertEquals('o', buffer[0]);
        Assert.assertEquals(-1, snapshot.read(buffer));
    }

    @Test
    public void testRead2() {
        MemorySnapshot snapshot = new MemorySnapshot("Hello,".getBytes());
        byte[] buffer = new byte[2];
        Assert.assertEquals(2, snapshot.read(buffer));
        Assert.assertArrayEquals(new byte[]{'H', 'e'}, buffer);
        Assert.assertEquals(2, snapshot.read(buffer));
        Assert.assertArrayEquals(new byte[]{'l', 'l'}, buffer);
        Assert.assertEquals(2, snapshot.read(buffer));
        Assert.assertArrayEquals(new byte[]{'o', ','}, buffer);
        Assert.assertEquals(-1, snapshot.read(buffer));
    }

}