package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MemoryEntrySequenceTest {

    @Test
    public void testAppendEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 1));
        Assert.assertEquals(2, sequence.getNextLogIndex());
        Assert.assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testAppendEntries() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.append(Arrays.asList(
                new NoOpEntry(1, 1),
                new NoOpEntry(2, 1)
        ));
        Assert.assertEquals(3, sequence.getNextLogIndex());
        Assert.assertEquals(2, sequence.getLastLogIndex());
    }

    @Test
    public void testGetEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        Assert.assertNull(sequence.getEntry(1));
        Assert.assertEquals(2, sequence.getEntry(2).getIndex());
        Assert.assertEquals(3, sequence.getEntry(3).getIndex());
        Assert.assertNull(sequence.getEntry(4));
    }

    @Test
    public void testGetEntryMeta() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        Assert.assertNull(sequence.getEntry(2));
        sequence.append(new NoOpEntry(2, 1));
        EntryMeta meta = sequence.getEntryMeta(2);
        Assert.assertNotNull(meta);
        Assert.assertEquals(2, meta.getIndex());
        Assert.assertEquals(1, meta.getTerm());
    }

    @Test
    public void testIsEntryPresent() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(1);
        Assert.assertFalse(sequence.isEntryPresent(1));
        sequence.append(new NoOpEntry(1, 1));
        Assert.assertTrue(sequence.isEntryPresent(1));
        Assert.assertFalse(sequence.isEntryPresent(0));
        Assert.assertFalse(sequence.isEntryPresent(2));
    }

    @Test(expected = EmptySequenceException.class)
    public void testSubListEmpty() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        Assert.assertTrue(sequence.subList(2, 2).isEmpty());
    }

    @Test
    public void testSubListResultEmpty() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(new NoOpEntry(2, 1));
        sequence.subList(2, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubListOutOfIndex() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(new NoOpEntry(2, 1));
        sequence.subList(1, 3);
    }

    @Test
    public void testSubListOneElement() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        List<Entry> subList = sequence.subList(2, 3);
        Assert.assertEquals(1, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
    }

    @Test
    public void testSubViewEmpty() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        Assert.assertTrue(sequence.subView(2).isEmpty());
    }

    @Test
    public void testSubView() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        List<Entry> subList = sequence.subView(2);
        Assert.assertEquals(2, subList.size());
        Assert.assertEquals(2, subList.get(0).getIndex());
        Assert.assertEquals(3, subList.get(1).getIndex());
    }

    @Test
    public void testSubView2() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        List<Entry> subList = sequence.subView(4);
        Assert.assertEquals(0, subList.size());
    }

    @Test
    public void testSubView3() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        List<Entry> subList = sequence.subView(1);
        Assert.assertEquals(2, subList.size());
    }

    @Test
    public void testBuildGroupConfigList() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new AddNodeEntry(2, 1, Collections.emptySet(), new NodeEndpoint("A", "localhost", 2333)),
                new NoOpEntry(3, 1),
                new RemoveNodeEntry(4, 1, Collections.emptySet(), new NodeId("A"))
        ));
        GroupConfigEntryList list = sequence.buildGroupConfigEntryList(Collections.emptySet());
        Iterator<GroupConfigEntry> iterator = list.iterator();
        Assert.assertEquals(2, iterator.next().getIndex());
        Assert.assertEquals(4, iterator.next().getIndex());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testRemoveAfterEmpty() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.removeAfter(1);
    }

    @Test
    public void testRemoveAfterNoAction() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        sequence.removeAfter(3);
        Assert.assertEquals(3, sequence.getLastLogIndex());
        Assert.assertEquals(4, sequence.getNextLogIndex());
    }

    @Test
    public void testRemoveAfterPartial() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        sequence.removeAfter(2);
        Assert.assertEquals(2, sequence.getLastLogIndex());
        Assert.assertEquals(3, sequence.getNextLogIndex());
    }

    @Test
    public void testRemoveAfterAll() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        Assert.assertNotNull(sequence.getEntry(2));
        sequence.removeAfter(1);
        Assert.assertTrue(sequence.isEmpty());
        Assert.assertEquals(2, sequence.getNextLogIndex());
    }

}
