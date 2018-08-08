package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.entry.NoOpEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MemoryEntrySequenceTest {

    @Test
    public void testAppendEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.append(new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1));
        Assert.assertEquals(2, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testAppendEntries() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        Assert.assertEquals(3, sequence.getNextLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
    }

    @Test
    public void testGetEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        Assert.assertEquals(2, sequence.getEntry(2).getIndex());
        Assert.assertEquals(3, sequence.getEntry(3).getIndex());
        Assert.assertNull(sequence.getEntry(4));
    }

    @Test
    public void testSubListEmpty() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        List<Entry> subList = sequence.subList(2, 2);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubListEmpty2() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        List<Entry> subList = sequence.subList(4, 4);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubListOneElement() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        List<Entry> subList = sequence.subList(2, 3);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
    }

    @Test
    public void testSubListAll() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        List<Entry> subList = sequence.subList(2);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
        Assert.assertEquals(3, subList.get(1).getIndex());
    }

    @Test
    public void testRemoveAfterNoAction() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        sequence.removeAfter(4);
        Assert.assertEquals(3, sequence.getLastLogIndex());
        Assert.assertEquals(4, sequence.getNextLogIndex());
    }

    @Test
    public void testRemoveAfterPartial() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        sequence.removeAfter(2);
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertEquals(3, sequence.getNextLogIndex());
    }

    @Test
    public void testRemoveAfterAll() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1),
                new NoOpEntry(sequence.getAndIncreaseNextLogIndex(), 1)
        ));
        Assert.assertNotNull(sequence.getEntry(2));
        sequence.removeAfter(1);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(2, sequence.getNextLogIndex());
    }

}
