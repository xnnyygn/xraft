package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.MemorySnapshot;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryLogTest {

    private EntryApplyRecorder entryApplyRecorder;
    private MemoryLog log;

    @Before
    public void setUp() {
        entryApplyRecorder = new EntryApplyRecorder();
        log = new MemoryLog();
        log.setStateMachine(entryApplyRecorder);
    }

    @Test
    public void testCreateAppendEntriesRpcNoLog() {
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 1, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(0, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcOneLogEntry() {
        log.appendEntry(1); // 1
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 2, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntries1() {
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 2, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(1, rpc.getEntries().size());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntries2() {
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 3, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(2, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
    }

    @Test
    public void testIsNewerThanNoLog() {
        Assert.assertFalse(log.isNewerThan(0, 0));
    }

    @Test
    public void testIsNewerThanSame() {
        log.appendEntry(1);
        Assert.assertFalse(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanHighTerm() {
        log.appendEntry(2); // index = 1, term = 2
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanMoreLog() {
        log.appendEntry(1);
        log.appendEntry(1); // index = 2, term = 1
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

    @Test
    public void testGetLastEntryMetaNoLogAndSnapshot() {
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(0, lastEntryMeta.getIndex());
        Assert.assertEquals(0, lastEntryMeta.getTerm());
    }

    @Test
    public void testGetLastEntryMetaNoLog() {
        MemoryLog log = new MemoryLog(new MemorySnapshot(1, 2), new MemoryEntrySequence(2), new EventBus());
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(1, lastEntryMeta.getIndex());
        Assert.assertEquals(2, lastEntryMeta.getTerm());
    }

    @Test
    public void testGetLastEntryMetaNoSnapshot() {
        log.appendEntry(1);
        log.appendEntry(1);
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(2, lastEntryMeta.getIndex());
        Assert.assertEquals(1, lastEntryMeta.getTerm());
    }

}
