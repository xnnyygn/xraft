package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.support.ByteArraySeekableFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileEntrySequenceTest {

    private FileEntrySequence sequence;
    private ByteArraySeekableFile entriesFile;
    private ByteArraySeekableFile entryOffsetIndexFile;

    @Before
    public void setUp() throws Exception {
        entriesFile = new ByteArraySeekableFile();
        entryOffsetIndexFile = new ByteArraySeekableFile();
        sequence = new FileEntrySequence(entriesFile, entryOffsetIndexFile, 1);
    }

    @Test
    public void testAppendEntry() throws IOException {
        Assert.assertEquals(1, sequence.getNextLogIndex());
        Assert.assertTrue(sequence.isEmpty());

        GeneralEntry entry = new GeneralEntry(sequence.getAndIncreaseNextLogIndex(), 1, "entry-1".getBytes());
        sequence.append(entry);

        Assert.assertEquals(2, sequence.getNextLogIndex());
        Assert.assertFalse(sequence.isEmpty());
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());

        sequence.commit(entry.getIndex());

        Assert.assertEquals(1, sequence.getLastEntryIndexInFile());

        // entry length
        // (kind, 4) + (index, 4) + (term, 4) + (data length, 4) + (data, 7) = 23
        Assert.assertEquals(23, entriesFile.size());
        entriesFile.seek(0);
        Assert.assertEquals(entry.getKind(), entriesFile.readInt());
        Assert.assertEquals(entry.getIndex(), entriesFile.readInt());
        Assert.assertEquals(entry.getTerm(), entriesFile.readInt());
        int dataLength = entry.getCommandBytes().length;
        Assert.assertEquals(dataLength, entriesFile.readInt());
        byte[] data = new byte[dataLength];
        entriesFile.read(data);
        Assert.assertArrayEquals(entry.getCommandBytes(), data);

        // entry offset index
        // (min entry index, 4) + (max entry index 4) + (entry offset, 8) = 16
        Assert.assertEquals(16, entryOffsetIndexFile.size());
        entryOffsetIndexFile.seek(0);
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(0L, entryOffsetIndexFile.readLong());
    }

    @Test
    public void testGetLastEntry() {
        Assert.assertNull(sequence.getLastEntry());

        GeneralEntry entry = new GeneralEntry(sequence.getAndIncreaseNextLogIndex(), 1, "entry-0".getBytes());
        sequence.append(entry);

        Entry lastEntry = sequence.getLastEntry();
        Assert.assertNotNull(lastEntry);
        Assert.assertEquals(entry.getIndex(), lastEntry.getIndex());

        sequence.commit(entry.getIndex());

        lastEntry = sequence.getLastEntry();
        Assert.assertNotNull(lastEntry);
        Assert.assertEquals(entry.getIndex(), lastEntry.getIndex());
    }

    @Test
    public void testAppendEntries() throws IOException {
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)
        ));
        Assert.assertEquals(4, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());

        sequence.commit(2);

        Assert.assertEquals(2, sequence.getLastEntryIndexInFile());
        Assert.assertEquals(32, entriesFile.size());
        Assert.assertEquals(24, entryOffsetIndexFile.size());
        entryOffsetIndexFile.seek(0);
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(2, entryOffsetIndexFile.readInt());
        Assert.assertEquals(0L, entryOffsetIndexFile.readLong());
        Assert.assertEquals(16L, entryOffsetIndexFile.readLong());
    }


    @Test(expected = IllegalStateException.class)
    public void testRemoveAfterNoEntry() {
        sequence.removeAfter(1);
    }

    @Test
    public void testRemoveAfterLargerThanLastLogIndex() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());

        sequence.removeAfter(1);

        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    // (no entry in file) : (1 pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex1() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());

        sequence.removeAfter(0);

        Assert.assertTrue(sequence.isEmpty());
    }

    // (1 entry in file) : (no pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex2() throws IOException {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        sequence.commit(1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        Assert.assertTrue(entryOffsetIndexFile.size() > 0);

        sequence.removeAfter(0);

        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(0, entriesFile.size());
        Assert.assertEquals(0, entryOffsetIndexFile.size());
    }

    // (1 entry in file) : (1 pending entry)
    @Test
    public void testRemoveAfterSmallerThanFirstLogIndex3() throws IOException {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        sequence.commit(1);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        Assert.assertTrue(entryOffsetIndexFile.size() > 0);

        sequence.removeAfter(0);

        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(0, entriesFile.size());
        Assert.assertEquals(0, entryOffsetIndexFile.size());
    }

    @Test
    public void testRemoveAfterPendingEntries1() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());

        sequence.removeAfter(0);

        Assert.assertTrue(sequence.isEmpty());
    }

    @Test
    public void testRemoveAfterPendingEntries2() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2));
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());

        sequence.removeAfter(1);

        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testRemoveAfterEntriesInFile1() throws IOException {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 3
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        Assert.assertTrue(entryOffsetIndexFile.size() > 0);

        sequence.removeAfter(2);

        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        Assert.assertTrue(entryOffsetIndexFile.size() > 0);
    }

    @Test
    public void testRemoveAfterEntriesInFile2() throws IOException {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 3
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        entryOffsetIndexFile.seek(0);
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(2, entryOffsetIndexFile.readInt());
        Assert.assertEquals(0L, entryOffsetIndexFile.readLong());
        // (kind, 4) + (index, 4) + (term, 4) + (data length, 4) = 16
        Assert.assertEquals(16L, entryOffsetIndexFile.readLong());

        sequence.removeAfter(1);

        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
        Assert.assertTrue(entriesFile.size() > 0);
        entryOffsetIndexFile.seek(0);
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(1, entryOffsetIndexFile.readInt());
        Assert.assertEquals(0L, entryOffsetIndexFile.readLong());
    }

    @Test
    public void testLoadEmpty() {
        sequence.load();

        Assert.assertEquals(1, sequence.getNextLogIndex());
        Assert.assertTrue(sequence.isEmpty());
    }

    @Test
    public void testLoad() throws IOException {
        for (int i = 1; i <= 2; i++) {
            entriesFile.writeInt(0); // kind
            entriesFile.writeInt(1); // index
            entriesFile.writeInt(1); // term
            entriesFile.writeInt(0); // data length
        }
        entriesFile.seek(0);

        entryOffsetIndexFile.writeInt(1); // min entry index
        entryOffsetIndexFile.writeInt(2); // max entry index
        entryOffsetIndexFile.writeLong(0L); // entry 1 offset
        entryOffsetIndexFile.writeLong(16L); // entry 2 offset
        entryOffsetIndexFile.seek(0);

        sequence.load();

        Assert.assertEquals(3, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
    }

    @Test
    public void testSubList1() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(1);
        Assert.assertEquals(4, subList.size());
        Assert.assertEquals(1, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(3).getIndex());
    }

    @Test
    public void testSubList2() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(2);
        Assert.assertEquals(3, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(2).getIndex());
    }

    @Test
    public void testSubList3() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(3);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(3, subList.get(0).getIndex());
        Assert.assertEquals(4, subList.get(1).getIndex());
    }

    @Test
    public void testSubList4() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(4);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(4, subList.get(0).getIndex());
    }

    @Test
    public void testSubList5() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(5);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubList6() {
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)); // 1
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 2)); // 2
        sequence.commit(2);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 3)); // 3
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 4)); // 4

        List<Entry> subList = sequence.subList(3, 4);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(3, subList.get(0).getIndex());
    }

    @Test
    public void testGetEntry() {
        sequence = new FileEntrySequence(entriesFile, entryOffsetIndexFile, 4);
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        Assert.assertEquals(1, sequence.getEntry(4).getTerm());
    }

    @After
    public void tearDown() throws Exception {
        sequence.close();
    }

}