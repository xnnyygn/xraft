package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryLogTest {

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
    public void testCreateAppendEntriesRpcNoLog() {
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 1, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(0, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcOneLogEntry() {
        log.appendEntry(1, new byte[0], null);
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 2, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntries() {
        log.appendEntry(1, new byte[0], null);
        log.appendEntry(1, new byte[0], null);
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 2, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(1, rpc.getEntries().size());
    }

    @Test
    public void testIsNewerThanNoLog() {
        Assert.assertFalse(log.isNewerThan(0, 0));
    }

    @Test
    public void testIsNewerThanSame() {
        log.appendEntry(1, new byte[0]);
        Assert.assertFalse(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanHighTerm() {
        log.appendEntry(2, new byte[0]);
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanMoreLog() {
        log.appendEntry(1, new byte[0]);
        log.appendEntry(1, new byte[0]);
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

}
