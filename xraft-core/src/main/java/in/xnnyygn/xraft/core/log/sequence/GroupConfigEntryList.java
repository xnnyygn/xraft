package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class GroupConfigEntryList implements Iterable<GroupConfigEntry> {

    private final LinkedList<GroupConfigEntry> entries = new LinkedList<>();

    public GroupConfigEntry getLast() {
        return entries.isEmpty() ? null : entries.getLast();
    }

    public void add(GroupConfigEntry entry) {
        entries.add(entry);
    }

    /**
     * Remove entries whose index is greater than {@code entryIndex}.
     *
     * @param entryIndex entry index
     * @return first removed entry, {@code null} if no entry removed
     */
    public GroupConfigEntry removeAfter(int entryIndex) {
        Iterator<GroupConfigEntry> iterator = entries.iterator();
        GroupConfigEntry firstRemovedEntry = null;
        while (iterator.hasNext()) {
            GroupConfigEntry entry = iterator.next();
            if (entry.getIndex() > entryIndex) {
                if (firstRemovedEntry == null) {
                    firstRemovedEntry = entry;
                }
                iterator.remove();
            }
        }
        return firstRemovedEntry;
    }

    // TODO add test
    public List<GroupConfigEntry> subList(int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("from index > to index");
        }
        return entries.stream()
                .filter(e -> e.getIndex() >= fromIndex && e.getIndex() < toIndex)
                .collect(Collectors.toList());
    }

    @Override
    @Nonnull
    public Iterator<GroupConfigEntry> iterator() {
        return entries.iterator();
    }

    @Override
    public String toString() {
        return "GroupConfigEntryList{" + entries + '}';
    }

}
