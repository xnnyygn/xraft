package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryAppendEvent extends AbstractEntryEvent {

    public GroupConfigEntryAppendEvent(GroupConfigEntry entry) {
        super(entry);
    }

}
