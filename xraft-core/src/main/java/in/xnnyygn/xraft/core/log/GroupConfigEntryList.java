package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

import java.util.LinkedList;

public class GroupConfigEntryList {

    private final LinkedList<GroupConfigEntry> entries = new LinkedList<>();

    public GroupConfigEntry getLast() {
        return entries.isEmpty() ? null : entries.getLast();
    }

    public void add(GroupConfigEntry entry) {
        entries.add(entry);
    }

//    public void rollback() {
//        if (this.entries.size() <= 1) {
//            throw new IllegalStateException("cannot rollback first entry");
//        }
//        this.entries.removeLast();
//    }

    @Override
    public String toString() {
        return "GroupConfigEntryList{" + entries + '}';
    }

}
