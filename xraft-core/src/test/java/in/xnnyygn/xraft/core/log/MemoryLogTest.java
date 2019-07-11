package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.entry.NoOpEntry;
import in.xnnyygn.xraft.core.log.sequence.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.log.snapshot.MemorySnapshot;
import in.xnnyygn.xraft.core.log.statemachine.EmptyStateMachine;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MemoryLogTest {

    @Test
    public void testGetLastEntryMetaNoLogAndSnapshot() {
        MemoryLog log = new MemoryLog();
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(Entry.KIND_NO_OP, lastEntryMeta.getKind());
        Assert.assertEquals(0, lastEntryMeta.getIndex());
        Assert.assertEquals(0, lastEntryMeta.getTerm());
    }

    @Test
    public void testGetLastEntryMetaNoLog() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 2),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(3, lastEntryMeta.getIndex());
        Assert.assertEquals(2, lastEntryMeta.getTerm());
    }

    @Test
    public void testGetLastEntryMetaNoSnapshot() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        EntryMeta lastEntryMeta = log.getLastEntryMeta();
        Assert.assertEquals(2, lastEntryMeta.getIndex());
        Assert.assertEquals(1, lastEntryMeta.getTerm());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateAppendEntriesIllegalNextIndex() {
        MemoryLog log = new MemoryLog();
        log.createAppendEntriesRpc(1, new NodeId("A"), 2, Log.ALL_ENTRIES);
    }

    @Test
    public void testCreateAppendEntriesRpcNoLog() {
        MemoryLog log = new MemoryLog();
        NodeId nodeId = new NodeId("A");
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, nodeId, 1, Log.ALL_ENTRIES
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(nodeId, rpc.getLeaderId());
        Assert.assertEquals(0, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcStartFromOne() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 1, Log.ALL_ENTRIES
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(0, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getPrevLogTerm());
        Assert.assertEquals(2, rpc.getEntries().size());
        Assert.assertEquals(1, rpc.getEntries().get(0).getIndex());
    }

    @Test
    public void testCreateAppendEntriesRpcOneLogEntry() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 2, Log.ALL_ENTRIES
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
        Assert.assertEquals(0, rpc.getLeaderCommit());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntriesFrom2() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 2, Log.ALL_ENTRIES
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(1, rpc.getPrevLogIndex());
        Assert.assertEquals(1, rpc.getEntries().size());
        Assert.assertEquals(2, rpc.getEntries().get(0).getIndex());
    }

    @Test
    public void testCreateAppendEntriesRpcTwoLogEntriesFrom3() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 3, Log.ALL_ENTRIES
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(2, rpc.getPrevLogIndex());
        Assert.assertEquals(0, rpc.getEntries().size());
    }

    @Test
    public void testCreateAppendEntriesRpcLimit1() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1); // 3
        log.appendEntry(1); // 4
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 3, 1
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(2, rpc.getPrevLogIndex());
        Assert.assertEquals(1, rpc.getEntries().size());
        Assert.assertEquals(3, rpc.getEntries().get(0).getIndex());
    }

    @Test
    public void testCreateAppendEntriesRpcLimit2() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1); // 3
        log.appendEntry(1); // 4
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 3, 2
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(2, rpc.getPrevLogIndex());
        Assert.assertEquals(2, rpc.getEntries().size());
        Assert.assertEquals(3, rpc.getEntries().get(0).getIndex());
        Assert.assertEquals(4, rpc.getEntries().get(1).getIndex());
    }

    @Test
    public void testCreateAppendEntriesRpcLimit3() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1); // 3
        log.appendEntry(1); // 4
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                1, new NodeId("A"), 3, 3
        );
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(2, rpc.getPrevLogIndex());
        Assert.assertEquals(2, rpc.getEntries().size());
        Assert.assertEquals(3, rpc.getEntries().get(0).getIndex());
        Assert.assertEquals(4, rpc.getEntries().get(1).getIndex());
    }

    @Test
    public void testCreateAppendEntriesUseSnapshot() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 2),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(
                2, new NodeId("A"), 4, Log.ALL_ENTRIES
        );
        Assert.assertEquals(2, rpc.getTerm());
        Assert.assertEquals(3, rpc.getPrevLogIndex());
        Assert.assertEquals(2, rpc.getPrevLogTerm());
        Assert.assertEquals(0, rpc.getEntries().size());
    }

    @Test(expected = EntryInSnapshotException.class)
    public void testCreateAppendEntriesLogEmptyEntryInSnapshot() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 2),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        log.createAppendEntriesRpc(
                2, new NodeId("A"), 3, Log.ALL_ENTRIES
        );
    }

    @Test(expected = EntryInSnapshotException.class)
    public void testCreateAppendEntriesLogNotEmptyEntryInSnapshot() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 2),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        log.appendEntry(1); // 4
        log.createAppendEntriesRpc(
                2, new NodeId("A"), 3, Log.ALL_ENTRIES
        );
    }

    @Test
    public void testCreateInstallSnapshotRpcEmpty() {
        MemoryLog log = new MemoryLog();
        NodeId nodeId = new NodeId("A");
        InstallSnapshotRpc rpc = log.createInstallSnapshotRpc(1, nodeId, 0, 10);
        Assert.assertEquals(1, rpc.getTerm());
        Assert.assertEquals(nodeId, rpc.getLeaderId());
        Assert.assertEquals(0, rpc.getLastIndex());
        Assert.assertEquals(0, rpc.getLastTerm());
        Assert.assertEquals(0, rpc.getOffset());
        Assert.assertArrayEquals(new byte[0], rpc.getData());
        Assert.assertTrue(rpc.isDone());
    }

    @Test
    public void testCreateInstallSnapshotRpc() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4, "test".getBytes(), Collections.emptySet()),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        InstallSnapshotRpc rpc = log.createInstallSnapshotRpc(4, new NodeId("A"), 0, 2);
        Assert.assertEquals(3, rpc.getLastIndex());
        Assert.assertEquals(4, rpc.getLastTerm());
        Assert.assertArrayEquals("te".getBytes(), rpc.getData());
        Assert.assertFalse(rpc.isDone());
    }

    @Test
    public void testGetLastUncommittedGroupConfigEntry() {
        MemoryLog log = new MemoryLog();
        log.appendEntryForAddNode(1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333));
        log.appendEntryForRemoveNode(1, Collections.emptySet(), new NodeId("A"));
        GroupConfigEntry entry = log.getLastGroupConfigEntry();
        Assert.assertNotNull(entry);
        Assert.assertEquals(Entry.KIND_REMOVE_NODE, entry.getKind());
        Assert.assertEquals(2, entry.getIndex());

    }

    @Test
    public void testGetLastUncommittedGroupConfigEntryEmpty() {
        MemoryLog log = new MemoryLog();
        Assert.assertNull(log.getLastGroupConfigEntry());
    }

    @Test
    public void testGetLastUncommittedGroupConfigEntryCommitted() {
        MemoryLog log = new MemoryLog();
        log.appendEntryForAddNode(1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333));
        log.advanceCommitIndex(1, 1);
        Assert.assertNotNull(log.getLastGroupConfigEntry());
    }

    @Test
    public void testGetNextLogEmpty() {
        MemoryLog log = new MemoryLog();
        Assert.assertEquals(1, log.getNextIndex());
    }

    @Test
    public void testGetNextLog() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        Assert.assertEquals(4, log.getNextIndex());
    }

    @Test
    public void testIsNewerThanNoLog() {
        MemoryLog log = new MemoryLog();
        Assert.assertFalse(log.isNewerThan(0, 0));
    }

    @Test
    public void testIsNewerThanSame() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // index = 1, term = 1
        Assert.assertFalse(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanHighTerm() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(2); // index = 1, term = 2
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

    @Test
    public void testIsNewerThanMoreLog() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.appendEntry(1); // index = 2, term = 1
        Assert.assertTrue(log.isNewerThan(1, 1));
    }

    @Test
    public void testAppendEntryForAddNode() {
        MemoryLog log = new MemoryLog();
        Assert.assertNull(log.getLastGroupConfigEntry());
        log.appendEntryForAddNode(1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333));
        Assert.assertNotNull(log.getLastGroupConfigEntry());
    }

    @Test
    public void testAppendEntryForRemoveNode() {
        MemoryLog log = new MemoryLog();
        Assert.assertNull(log.getLastGroupConfigEntry());
        log.appendEntryForRemoveNode(1, Collections.emptySet(), new NodeId("A"));
        Assert.assertNotNull(log.getLastGroupConfigEntry());
    }

    @Test
    public void testAppendEntriesFromLeaderNoLog() {
        MemoryLog log = new MemoryLog();
        AppendEntriesState state = log.appendEntriesFromLeader(0, 0, Arrays.asList(
                new NoOpEntry(1, 1),
                new NoOpEntry(2, 1)
        ));
        Assert.assertTrue(state.isSuccess());
        Assert.assertEquals(3, log.getNextIndex());
    }

    // prevLogIndex == snapshot.lastIncludedIndex
    // prevLogTerm == snapshot.lastIncludedTerm
    @Test
    public void testAppendEntriesFromLeaderSnapshot1() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        AppendEntriesState state = log.appendEntriesFromLeader(3, 4, Collections.emptyList());
        Assert.assertTrue(state.isSuccess());
    }

    // prevLogIndex == snapshot.lastIncludedIndex
    // prevLogTerm != snapshot.lastIncludedTerm
    @Test
    public void testAppendEntriesFromLeaderSnapshot2() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        AppendEntriesState state = log.appendEntriesFromLeader(3, 5, Collections.emptyList());
        Assert.assertFalse(state.isSuccess());
    }

    // prevLogIndex < snapshot.lastIncludedIndex
    @Test
    public void testAppendEntriesFromLeaderSnapshot3() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 4, Collections.emptyList());
        Assert.assertFalse(state.isSuccess());
    }

    @Test
    public void testAppendEntriesFromLeaderPrevLogNotFound() {
        MemoryLog log = new MemoryLog();
        Assert.assertEquals(1, log.getNextIndex());
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, Collections.emptyList());
        Assert.assertFalse(state.isSuccess());
    }

    @Test
    public void testAppendEntriesFromLeaderPrevLogTermNotMatch() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        AppendEntriesState state = log.appendEntriesFromLeader(1, 2, Collections.emptyList());
        Assert.assertFalse(state.isSuccess());
    }

    // (index, term)
    // follower: (1, 1), (2, 1)
    // leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderSkip() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
    }

    @Test
    public void testAppendEntriesFromLeaderNoConflict() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
    }

    // follower: (1, 1), (2, 1)
    // leader  :         (2, 2), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict1() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 2),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
    }

    // follower: (1, 1), (2, 1), (3, 1)
    // leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict2() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1); // 3
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
    }

    // follower: (1, 1), (2, 1), (3, 1, no-op, committed)
    // leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict3() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1); // 3
        log.advanceCommitIndex(3, 1);
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
    }

    // follower: (1, 1), (2, 1), (3, 1, general, committed)
    // leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict4() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntry(1, "test".getBytes()); // 3
        log.advanceCommitIndex(3, 1);
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
        Assert.assertEquals(2, log.getCommitIndex());
    }

    // follower: (1, 1), (2, 1), (3, 1, group-config, committed)
    // leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict5() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1); // 1
        log.appendEntry(1); // 2
        log.appendEntryForRemoveNode(1, Collections.emptySet(), NodeId.of("A")); // 3
        log.advanceCommitIndex(3, 1);
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
        AppendEntriesState state = log.appendEntriesFromLeader(1, 1, leaderEntries);
        Assert.assertTrue(state.isSuccess());
        Assert.assertEquals(2, log.getCommitIndex());
    }

    @Test
    public void testAdvanceCommitIndexLessThanCurrentCommitIndex() {
        MemoryLog log = new MemoryLog();
        log.advanceCommitIndex(0, 1);
    }

    @Test
    public void testAdvanceCommitIndexEntryNotFound() {
        MemoryLog log = new MemoryLog();
        log.advanceCommitIndex(1, 1);
    }

    @Test
    public void testAdvanceCommitIndexNotCurrentTerm() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.advanceCommitIndex(1, 2);
    }

    @Test
    public void testAdvanceCommitIndex() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        Assert.assertEquals(0, log.commitIndex);
        log.advanceCommitIndex(1, 1);
        Assert.assertEquals(1, log.commitIndex);
    }

    @Test
    public void testAdvanceCommitIndexApplyEntries() {
        EmptyStateMachine stateMachine = new EmptyStateMachine();

        MemoryLog log = new MemoryLog();
        log.setStateMachine(stateMachine);
        log.appendEntry(1, "test".getBytes());
        log.appendEntry(1);
        Assert.assertEquals(0, stateMachine.getLastApplied());
        log.advanceCommitIndex(1, 1);
        Assert.assertEquals(1, stateMachine.getLastApplied());
    }

    @Test
    public void testAdvanceCommitIndexApplySnapshot() {
        EmptyStateMachine stateMachine = new EmptyStateMachine();

        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        log.setStateMachine(stateMachine);
        log.appendEntry(4, "test".getBytes()); // index 4
        Assert.assertEquals(0, stateMachine.getLastApplied());
        log.advanceCommitIndex(4, 4);
        Assert.assertEquals(4, stateMachine.getLastApplied());
    }

    @Test
    public void testAdvanceCommitIndexGenerateSnapshot() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.advanceCommitIndex(1, 1);
        log.generateSnapshot(1, Collections.emptySet());
        log.appendEntry(2);
    }

    @Test
    public void testInstallSnapshotLessThanLastIncludedIndex() {
        MemoryLog log = new MemoryLog(
                new MemorySnapshot(3, 4),
                new MemoryEntrySequence(4),
                new EventBus(),
                Collections.emptySet()
        );
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(2);
        rpc.setLastTerm(3);
        Assert.assertEquals(InstallSnapshotState.StateName.ILLEGAL_INSTALL_SNAPSHOT_RPC, log.installSnapshot(rpc).getStateName());
    }

    @Test
    public void testInstallSnapshot() {
        EmptyStateMachine stateMachine = new EmptyStateMachine();
        MemoryLog log = new MemoryLog();
        log.setStateMachine(stateMachine);
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(2);
        rpc.setLastTerm(3);
        rpc.setLastConfig(Collections.emptySet());
        rpc.setData(new byte[0]);
        rpc.setDone(true);
        Assert.assertEquals(0, log.commitIndex);
        Assert.assertEquals(0, stateMachine.getLastApplied());
        log.installSnapshot(rpc);
        Assert.assertEquals(2, log.commitIndex);
        Assert.assertEquals(2, stateMachine.getLastApplied());
    }

    @Test
    public void testInstallSnapshot2() {
        EmptyStateMachine stateMachine = new EmptyStateMachine();
        MemoryLog log = new MemoryLog();
        log.setStateMachine(stateMachine);
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(2);
        rpc.setLastTerm(3);
        rpc.setLastConfig(Collections.emptySet());
        rpc.setData(new byte[0]);
        rpc.setDone(false);
        log.installSnapshot(rpc);
        Assert.assertEquals(0, log.commitIndex);
        Assert.assertEquals(0, stateMachine.getLastApplied());
    }

}
