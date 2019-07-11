package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GroupConfigEntryListTest {

    @Test
    public void testGetLastEmpty() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        Assert.assertNull(list.getLast());
    }

    @Test
    public void testGetLastNormal() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        GroupConfigEntry lastEntry = list.getLast();
        Assert.assertNotNull(lastEntry);
        Assert.assertEquals(1, lastEntry.getIndex());
    }

    @Test
    public void removeAfter() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        list.add(new AddNodeEntry(4, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        list.add(new AddNodeEntry(10, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        GroupConfigEntry firstRemovedEntry = list.removeAfter(3);
        Assert.assertEquals(4, firstRemovedEntry.getIndex());
        GroupConfigEntry lastEntry = list.getLast();
        Assert.assertEquals(1, lastEntry.getIndex());
    }

    @Test
    public void removeAfterNoEntryRemoved() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        Assert.assertNull(list.removeAfter(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubListIllegalArgument() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        list.subList(2, 1);
    }

    @Test
    public void testSubList() {
        GroupConfigEntryList list = new GroupConfigEntryList(Collections.emptySet());
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        list.add(new AddNodeEntry(4, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        list.add(new AddNodeEntry(10, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        Assert.assertEquals(0, list.subList(1, 1).size());
        Assert.assertEquals(1, list.subList(10, 11).size());
        Assert.assertEquals(3, list.subList(1, 11).size());
        Assert.assertEquals(1, list.subList(1, 4).size());
        Assert.assertEquals(2, list.subList(1, 5).size());
        Assert.assertEquals(2, list.subList(4, 11).size());
        Assert.assertEquals(1, list.subList(4, 10).size());
    }

}