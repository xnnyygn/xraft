package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import org.junit.Assert;
import org.junit.Test;

public class MemorySnapshotBuilderTest {

    @Test
    public void testSimple() {
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(3);
        rpc.setLastTerm(2);
        rpc.setOffset(0);
        byte[] data = "test".getBytes();
        rpc.setData(data);
        rpc.setDone(true);
        MemorySnapshotBuilder builder = new MemorySnapshotBuilder(rpc);
        MemorySnapshot snapshot = builder.build();

        Assert.assertEquals(3, snapshot.getLastIncludedIndex());
        Assert.assertEquals(2, snapshot.getLastIncludedTerm());
        Assert.assertArrayEquals(data, snapshot.getData());
    }

    @Test
    public void testAppend() {
        InstallSnapshotRpc firstRpc = new InstallSnapshotRpc();
        firstRpc.setLastIndex(3);
        firstRpc.setLastTerm(2);
        firstRpc.setOffset(0);
        firstRpc.setData("test".getBytes());
        firstRpc.setDone(false);
        MemorySnapshotBuilder builder = new MemorySnapshotBuilder(firstRpc);

        InstallSnapshotRpc secondRpc = new InstallSnapshotRpc();
        secondRpc.setLastIndex(3);
        secondRpc.setLastTerm(2);
        secondRpc.setOffset(4);
        secondRpc.setData("foo".getBytes());
        secondRpc.setDone(true);
        builder.append(secondRpc);
        MemorySnapshot snapshot = builder.build();

        Assert.assertEquals(3, snapshot.getLastIncludedIndex());
        Assert.assertEquals(2, snapshot.getLastIncludedTerm());
        Assert.assertArrayEquals("testfoo".getBytes(), snapshot.getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendIllegalOffset() {
        InstallSnapshotRpc firstRpc = new InstallSnapshotRpc();
        firstRpc.setLastIndex(3);
        firstRpc.setLastTerm(2);
        firstRpc.setOffset(0);
        firstRpc.setData("test".getBytes());
        firstRpc.setDone(false);
        MemorySnapshotBuilder builder = new MemorySnapshotBuilder(firstRpc);

        InstallSnapshotRpc secondRpc = new InstallSnapshotRpc();
        secondRpc.setLastIndex(3);
        secondRpc.setLastTerm(2);
        secondRpc.setOffset(0);
        secondRpc.setData("foo".getBytes());
        secondRpc.setDone(true);
        builder.append(secondRpc);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendIllegalLastIncludedIndexOrTerm() {
        InstallSnapshotRpc firstRpc = new InstallSnapshotRpc();
        firstRpc.setLastIndex(3);
        firstRpc.setLastTerm(2);
        firstRpc.setOffset(0);
        firstRpc.setData("test".getBytes());
        firstRpc.setDone(false);
        MemorySnapshotBuilder builder = new MemorySnapshotBuilder(firstRpc);

        InstallSnapshotRpc secondRpc = new InstallSnapshotRpc();
        secondRpc.setLastIndex(2);
        secondRpc.setLastTerm(2);
        secondRpc.setOffset(4);
        secondRpc.setData("foo".getBytes());
        secondRpc.setDone(true);
        builder.append(secondRpc);
    }

}