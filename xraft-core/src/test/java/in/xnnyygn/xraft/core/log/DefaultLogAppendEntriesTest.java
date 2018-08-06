package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.GeneralEntry;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class DefaultLogAppendEntriesTest {

    private static final byte[] COMMAND = "command".getBytes();
    private EntryApplyRecorder entryApplyRecorder;
    private DefaultLog log;

    @Before
    public void setUp() {
        this.entryApplyRecorder = new EntryApplyRecorder();
        log = new DefaultLog();
        log.setEntryApplier(this.entryApplyRecorder);
    }

    @Test
    public void testAppendEntriesFresh() {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(1, 1, COMMAND),
                new GeneralEntry(2, 1, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
        Assert.assertEquals(3, log.getNextIndex());
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
        log.appendEntry(1, COMMAND);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(2);
        Assert.assertFalse(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesHeartbeat() {
        log.appendEntry(1, COMMAND);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesSkip() {
        log.appendEntry(1, COMMAND); // 1
        log.appendEntry(1, COMMAND); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 1, COMMAND),
                new GeneralEntry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesNoConflict() {
        log.appendEntry(1, COMMAND); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 1, COMMAND),
                new GeneralEntry(3, 1, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict1() {
        log.appendEntry(1, COMMAND); // 1
        log.appendEntry(1, COMMAND); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 2, COMMAND),
                new GeneralEntry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict2() {
        log.appendEntry(1, COMMAND); // 1
        log.appendEntry(1, COMMAND); // 2
        log.appendEntry(1, COMMAND); // 3

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 1, COMMAND),
                new GeneralEntry(3, 2, COMMAND)
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesApplyLeaderCommit() {
        log.appendEntry(1, COMMAND); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 2, COMMAND),
                new GeneralEntry(3, 2, COMMAND)
        ));
        rpc.setLeaderCommit(2);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2
        Assert.assertEquals(2, entryApplyRecorder.getEntries().size());
    }

    @Test
    public void testAppendEntriesApplyLastEntryIndex() {
        log.appendEntry(1, COMMAND); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(2);
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new GeneralEntry(2, 1, COMMAND),
                new GeneralEntry(3, 2, COMMAND)
        ));
        rpc.setLeaderCommit(4);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2, 3
        Assert.assertEquals(3, entryApplyRecorder.getEntries().size());
    }

}
