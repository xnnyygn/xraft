package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntrySequence;
import in.xnnyygn.xraft.core.log.entry.GeneralEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EntrySequenceTest {

    @Test
    public void testAppendEntry() {
        EntrySequence sequence = new EntrySequence();
        sequence.append(1, new byte[0]);
        Assert.assertEquals(2, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testAppendEntries() {
        EntrySequence sequence = new EntrySequence();
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(1, 1, new byte[0]),
                new GeneralEntry(2, 1, new byte[0])
        ));
        Assert.assertEquals(3, sequence.getNextLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
    }

    @Test
    public void testGetEntry() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        Assert.assertEquals(2, sequence.getEntry(2).getIndex());
        Assert.assertEquals(3, sequence.getEntry(3).getIndex());
        Assert.assertNull(sequence.getEntry(4));
    }

    @Test
    public void testSubListEmpty() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        List<Entry> subList = sequence.subList(2, 2);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubListEmpty2() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]), // 2
                new GeneralEntry(3, 1, new byte[0]) // 3
        ));
        List<Entry> subList = sequence.subList(4, 4);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubListOneElement() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        List<Entry> subList = sequence.subList(2, 3);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
    }

    @Test
    public void testSubListAll() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        List<Entry> subList = sequence.subList(2, sequence.getLastLogIndex() + 1);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
        Assert.assertEquals(3, subList.get(1).getIndex());
    }

    @Test
    public void testClearAfterNoAction() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        sequence.clearAfter(4);
        Assert.assertEquals(3, sequence.getLastLogIndex());
        Assert.assertEquals(4, sequence.getNextLogIndex());
    }

    @Test
    public void testClearAfterPartial() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]),
                new GeneralEntry(3, 1, new byte[0])
        ));
        sequence.clearAfter(2);
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertEquals(3, sequence.getNextLogIndex());
    }

    @Test
    public void testClearAfterAll() {
        EntrySequence sequence = new EntrySequence(2);
        sequence.appendEntries(Arrays.asList(
                new GeneralEntry(2, 1, new byte[0]), // 2
                new GeneralEntry(3, 1, new byte[0]) // 3
        ));
        Assert.assertNotNull(sequence.getEntry(2));
        sequence.clearAfter(1);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(2, sequence.getNextLogIndex());
    }

    @Test
    public void testClearBeforeFirstLogIndex() {
        EntrySequence sequence = new EntrySequence(1);
        sequence.append(1, new byte[0]); // 1
        sequence.append(2, new byte[0]); // 2
        sequence.append(3, new byte[0]); // 3
        sequence.clearBefore(1);
        Assert.assertEquals(1, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());
    }

    @Test
    public void testClearBeforeMiddle() {
        EntrySequence sequence = new EntrySequence(1);
        sequence.append(1, new byte[0]); // 1
        sequence.append(2, new byte[0]); // 2
        sequence.append(3, new byte[0]); // 3
        sequence.clearBefore(2);
        Assert.assertEquals(2, sequence.getFirstLogIndex());
        Assert.assertEquals(3, sequence.getLastLogIndex());
    }

    @Test
    public void testClearBeforeLastLog() {
        EntrySequence sequence = new EntrySequence(1);
        sequence.append(1, new byte[0]); // 1
        sequence.append(2, new byte[0]); // 2
        sequence.append(3, new byte[0]); // 3
        sequence.clearBefore(4);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(4, sequence.getNextLogIndex());
    }

}
