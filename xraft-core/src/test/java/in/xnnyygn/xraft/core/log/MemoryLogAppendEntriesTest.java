package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class MemoryLogAppendEntriesTest {

    private ApplyEntryRecorder applyEntryRecorder;
    private MemoryLog log;

    @Before
    public void setUp() throws Exception {
        EventBus eventBus = new EventBus();
        applyEntryRecorder = new ApplyEntryRecorder();
        eventBus.register(applyEntryRecorder);
        log = new MemoryLog(eventBus);
    }

    @Test
    public void testAppendEntriesFresh() {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setEntries(Arrays.asList(
                new Entry(1, 1, new byte[0]),
                new Entry(2, 1, new byte[0])
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
        log.appendEntry(1, new byte[0], null);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(2);
        Assert.assertFalse(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesHeartbeat() {
        log.appendEntry(1, new byte[0], null);

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesSkip() {
        log.appendEntry(1, new byte[0], null); // 1
        log.appendEntry(1, new byte[0], null); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, new byte[0]),
                new Entry(3, 2, new byte[0])
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesNoConflict() {
        log.appendEntry(1, new byte[0], null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, new byte[0]),
                new Entry(3, 1, new byte[0])
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict1() {
        log.appendEntry(1, new byte[0], null); // 1
        log.appendEntry(1, new byte[0], null); // 2

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 2, new byte[0]),
                new Entry(3, 2, new byte[0])
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesConflict2() {
        log.appendEntry(1, new byte[0], null); // 1
        log.appendEntry(1, new byte[0], null); // 2
        log.appendEntry(1, new byte[0], null); // 3

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, new byte[0]),
                new Entry(3, 2, new byte[0])
        ));
        Assert.assertTrue(log.appendEntries(rpc));
    }

    @Test
    public void testAppendEntriesApplyLeaderCommit() {
        log.appendEntry(1, new byte[0], null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, new byte[0]),
                new Entry(3, 2, new byte[0])
        ));
        rpc.setLeaderCommit(2);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2
        Assert.assertEquals(2, applyEntryRecorder.getMessages().size());
    }

    @Test
    public void testAppendEntriesApplyLastEntryIndex() {
        log.appendEntry(1, new byte[0], null); // 1

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(1);
        rpc.setPrevLogTerm(1);
        rpc.setEntries(Arrays.asList(
                new Entry(2, 1, new byte[0]),
                new Entry(3, 2, new byte[0])
        ));
        rpc.setLeaderCommit(4);
        Assert.assertTrue(log.appendEntries(rpc));

        // 1, 2, 3
        Assert.assertEquals(3, applyEntryRecorder.getMessages().size());
    }

}
