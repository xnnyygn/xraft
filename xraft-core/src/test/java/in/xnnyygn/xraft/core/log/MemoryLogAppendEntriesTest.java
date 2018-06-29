package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class MemoryLogAppendEntriesTest {

    private static final byte[] COMMAND = "command".getBytes();
    private EntryApplyRecorder entryApplyRecorder;
    private MemoryLog log;

    @Before
    public void setUp() {
        this.entryApplyRecorder = new EntryApplyRecorder();
        log = new MemoryLog();
        log.setEntryApplier(this.entryApplyRecorder);
    }

    @Test
    public void testAppendEntriesFresh() {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setEntries(Arrays.asList(
                new Entry(1, 1, COMMAND),
                new Entry(2, 1, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
        Assert.assertEquals(2, log.getLastLogIndex());
    }

    @Test
    public void testAppendEntriesPrevLogNotFound() {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        Assert.assertFalse(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesPrevLogTermNotMatch() {
        log.appendEntry(1, COMMAND, null);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(2);
        Assert.assertFalse(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesHeartbeat() {
        log.appendEntry(1, COMMAND, null);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesSkip() {
        log.appendEntry(1, COMMAND, null); // 1
        log.appendEntry(1, COMMAND, null); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, COMMAND),
                new Entry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesNoConflict() {
        log.appendEntry(1, COMMAND, null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, COMMAND),
                new Entry(3, 1, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict1() {
        log.appendEntry(1, COMMAND, null); // 1
        log.appendEntry(1, COMMAND, null); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 2, COMMAND),
                new Entry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict2() {
        log.appendEntry(1, COMMAND, null); // 1
        log.appendEntry(1, COMMAND, null); // 2
        log.appendEntry(1, COMMAND, null); // 3

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, COMMAND),
                new Entry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesApplyLeaderCommit() {
        log.appendEntry(1, COMMAND, null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, COMMAND),
                new Entry(3, 2, COMMAND)
        ));
        rpc.setLeaderCommit(2);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2
        Assert.assertEquals(2, entryApplyRecorder.getEntries().size());
    }

    @Test
    public void testAppendEntriesApplyLastEntryIndex() {
        log.appendEntry(1, COMMAND, null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, COMMAND),
                new Entry(3, 2, COMMAND)
        ));
        rpc.setLeaderCommit(4);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2, 3
        Assert.assertEquals(3, entryApplyRecorder.getEntries().size());
    }

}
