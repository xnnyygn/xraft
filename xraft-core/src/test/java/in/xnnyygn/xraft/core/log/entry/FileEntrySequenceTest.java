package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.log.sequence.*;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.support.ByteArraySeekableFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FileEntrySequenceTest {

    private EntriesFile entriesFile;
    private EntryIndexFile entryIndexFile;

    @Before
    public void setUp() throws IOException {
        entriesFile = new EntriesFile(new ByteArraySeekableFile());
        entryIndexFile = new EntryIndexFile(new ByteArraySeekableFile());
    }

    @Test
    public void testInitializeEmpty() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 5);
        Assert.assertEquals(5, sequence.getNextLogIndex());
        Assert.assertTrue(sequence.isEmpty());
    }

    @Test
    public void testInitialize() throws IOException {
        entryIndexFile.appendEntryIndex(1, 0L, 1, 1);
        entryIndexFile.appendEntryIndex(2, 20L, 1, 1);
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertEquals(3, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertEquals(2, sequence.getCommitIndex());
    }

    @Test
    public void testBuildGroupConfigEntryListFromFile() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        appendEntryToFile(new GeneralEntry(2, 1, new byte[0]));
        appendEntryToFile(new AddNodeEntry(3, 1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        appendEntryToFile(new RemoveNodeEntry(4, 1, Collections.emptySet(), new NodeId("A")));

        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 5);
        GroupConfigEntryList list = sequence.buildGroupConfigEntryList(Collections.emptySet());
        Iterator<GroupConfigEntry> iterator = list.iterator();
        Assert.assertEquals(3, iterator.next().getIndex());
        Assert.assertEquals(4, iterator.next().getIndex());
        Assert.assertFalse(iterator.hasNext());
    }

    private void appendEntryToFile(Entry entry) throws IOException {
        long offset = entriesFile.appendEntry(entry);
        entryIndexFile.appendEntryIndex(entry.getIndex(), offset, entry.getKind(), entry.getTerm());
    }

    @Test
    public void testBuildGroupConfigEntryListFromPendingEntries() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        sequence.append(new GeneralEntry(2, 1, new byte[0]));
        sequence.append(new AddNodeEntry(3, 1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        sequence.append(new RemoveNodeEntry(4, 1, Collections.emptySet(), new NodeId("A")));
        GroupConfigEntryList list = sequence.buildGroupConfigEntryList(Collections.emptySet());
        Iterator<GroupConfigEntry> iterator = list.iterator();
        Assert.assertEquals(3, iterator.next().getIndex());
        Assert.assertEquals(4, iterator.next().getIndex());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testBuildGroupConfigEntryListBoth() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        appendEntryToFile(new AddNodeEntry(2, 1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new RemoveNodeEntry(3, 1, Collections.emptySet(), new NodeId("A")));
        GroupConfigEntryList list = sequence.buildGroupConfigEntryList(Collections.emptySet());
        Iterator<GroupConfigEntry> iterator = list.iterator();
        Assert.assertEquals(2, iterator.next().getIndex());
        Assert.assertEquals(3, iterator.next().getIndex());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testSubList1() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subView(1);
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(1, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(3).getIndex());
    }

    @Test
    public void testSubList2() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subView(2);
        Assert.assertEquals(3, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(2).getIndex());
    }

    @Test
    public void testSubList3() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subView(3);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(3, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(1).getIndex());
    }

    @Test
    public void testSubList4() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subView(4);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(4, subList.get(0).getIndex());
    }

    @Test
    public void testSubList5() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subView(5);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubList6() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(3, 4);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(3, subList.get(0).getIndex());
    }

    @Test
    public void testSubList7() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 2)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);

        List<Entry> subList = sequence.subList(1, 3);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(1, subList.get(0).getIndex());
        Assert.assertEquals(2, subList.get(1).getIndex());
    }

    @Test
    public void testGetEntry() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(2, 1));
        Assert.assertNull(sequence.getEntry(0));
        Assert.assertEquals(1, sequence.getEntry(1).getIndex());
        Assert.assertEquals(2, sequence.getEntry(2).getIndex());
        Assert.assertNull(sequence.getEntry(3));
    }

    @Test
    public void testGetEntryMetaNotFound() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertNull(sequence.getEntryMeta(2));
    }

    @Test
    public void testGetEntryMetaInPendingEntries() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        EntryMeta meta = sequence.getEntryMeta(1);
        Assert.assertNotNull(meta);
        Assert.assertEquals(Entry.KIND_NO_OP, meta.getKind());
        Assert.assertEquals(1, meta.getIndex());
        Assert.assertEquals(1, meta.getTerm());
    }

    @Test
    public void testGetEntryMetaInIndexFile() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        EntryMeta meta = sequence.getEntryMeta(1);
        Assert.assertNotNull(meta);
        Assert.assertEquals(Entry.KIND_NO_OP, meta.getKind());
        Assert.assertEquals(1, meta.getIndex());
        Assert.assertEquals(1, meta.getTerm());
    }

    @Test
    public void testGetLastEntryEmpty() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertNull(sequence.getLastEntry());
    }

    @Test
    public void testGetLastEntryFromFile() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertEquals(1, sequence.getLastEntry().getIndex());
    }

    @Test
    public void testGetLastEntryFromPendingEntries() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        Assert.assertEquals(1, sequence.getLastEntry().getIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendIllegalIndex() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(2, 1));
    }

    @Test
    public void testAppendEntry() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertEquals(1, sequence.getNextLogIndex());
        sequence.append(new NoOpEntry(1, 1));
        Assert.assertEquals(2, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getLastEntry().getIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitBeforeCommitIndex() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.commit(-1);
    }

    @Test
    public void testCommitDoNothing() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.commit(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitNoPendingEntry() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.commit(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitAfterLastLogIndex() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        sequence.commit(2);
    }

    @Test
    public void testCommitOne() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        sequence.append(new NoOpEntry(2, 1));
        Assert.assertEquals(0, sequence.getCommitIndex());
        Assert.assertEquals(0, entryIndexFile.getEntryIndexCount());
        sequence.commit(1);
        Assert.assertEquals(1, sequence.getCommitIndex());
        Assert.assertEquals(1, entryIndexFile.getEntryIndexCount());
        Assert.assertEquals(1, entryIndexFile.getMaxEntryIndex());
    }

    @Test
    public void testCommitMultiple() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        sequence.append(new NoOpEntry(2, 1));
        Assert.assertEquals(0, sequence.getCommitIndex());
        Assert.assertEquals(0, entryIndexFile.getEntryIndexCount());
        sequence.commit(2);
        Assert.assertEquals(2, sequence.getCommitIndex());
        Assert.assertEquals(2, entryIndexFile.getEntryIndexCount());
        Assert.assertEquals(2, entryIndexFile.getMaxEntryIndex());
    }

    @Test
    public void testRemoveAfterEmpty() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.removeAfter(1);
    }

    @Test
    public void testRemoveAfterLargerThanLastLogIndex() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(1, 1));
        sequence.removeAfter(1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    // (no entry in file) : (1 pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex1() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 1));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
        sequence.removeAfter(0);
        Assert.assertTrue(sequence.isEmpty());
    }

    // (1 entry in file) : (no pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex2() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
        sequence.removeAfter(0);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(0L, entriesFile.size());
        Assert.assertTrue(entryIndexFile.isEmpty());
    }

    // (1 entry in file) : (1 pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex3() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(2, 1));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
        sequence.removeAfter(0);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(0L, entriesFile.size());
        Assert.assertTrue(entryIndexFile.isEmpty());
    }

    @Test
    public void testRemoveAfterPendingEntries2() {
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 1));
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 2));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
        sequence.removeAfter(1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testRemoveAfterEntriesInFile2() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1)); // 1
        appendEntryToFile(new NoOpEntry(2, 1)); // 2
        FileEntrySequence sequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
        sequence.append(new NoOpEntry(3, 2)); // 3
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());
        sequence.removeAfter(1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

}