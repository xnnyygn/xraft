package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.EntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.MemorySnapshot;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryLogTest {

    private EntryApplyRecorder entryApplyRecorder;
    private MemoryLog log;

    @Before
    public void setUp() throws Exception {
        entryApplyRecorder = new EntryApplyRecorder();
        log = new MemoryLog();
        log.setEntryApplier(entryApplyRecorder);
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
        log.appendEntry(1, new byte[0]);
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, new NodeId("A"), 2, -1);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntries() {
        log.appendEntry(1, new byte[0]);
        log.appendEntry(1, new byte[0]);
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

    @Test
    public void testCreateRequestVoteRpcNoLogAndSnapshot() {
        MemoryLog log = new MemoryLog();
        NodeId nodeId = new NodeId("N");
        RequestVoteRpc rpc = log.createRequestVoteRpc(1, nodeId);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(nodeId, rpc.getCandidateId());
        Assert.assertEquals(0, rpc.getLastLogIndex());
        Assert.assertEquals(0, rpc.getLastLogTerm());
    }

    @Test
    public void testCreateRequestVoteRpcNoLog() {
        MemoryLog log = new MemoryLog(new MemorySnapshot(1, 2, new byte[0]), new EntrySequence(2), new EventBus());
        NodeId nodeId = new NodeId("N");
        RequestVoteRpc rpc = log.createRequestVoteRpc(3, nodeId);
        Assert.assertEquals(3, rpc.getTerm());
        Assert.assertEquals(nodeId, rpc.getCandidateId());
        Assert.assertEquals(1, rpc.getLastLogIndex());
        Assert.assertEquals(2, rpc.getLastLogTerm());
    }

    @Test
    public void testCreateRequestVoteRpcNoSnapshot() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        NodeId nodeId = new NodeId("N");
        RequestVoteRpc rpc = log.createRequestVoteRpc(2, nodeId);
        Assert.assertEquals(2, rpc.getTerm());
        Assert.assertEquals(nodeId, rpc.getCandidateId());
        Assert.assertEquals(1, rpc.getLastLogIndex());
        Assert.assertEquals(1, rpc.getLastLogTerm());
    }

}
