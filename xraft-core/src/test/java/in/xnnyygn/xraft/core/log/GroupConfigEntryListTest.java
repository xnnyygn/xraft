package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.sequence.GroupConfigEntryList;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GroupConfigEntryListTest {

    @Test
    public void testGetLastEmpty() {
        GroupConfigEntryList list = new GroupConfigEntryList();
        Assert.assertNull(list.getLast());
    }

    @Test
    public void testGetLastNormal() {
        GroupConfigEntryList list = new GroupConfigEntryList();
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        GroupConfigEntry lastEntry = list.getLast();
        Assert.assertNotNull(lastEntry);
        Assert.assertEquals(1, lastEntry.getIndex());
    }

    @Test
    public void removeAfter() {
        GroupConfigEntryList list = new GroupConfigEntryList();
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
        GroupConfigEntryList list = new GroupConfigEntryList();
        list.add(new AddNodeEntry(1, 1, Collections.<NodeEndpoint>emptySet(), new NodeEndpoint("A", "localhost", 2333)));
        Assert.assertNull(list.removeAfter(3));
    }

}