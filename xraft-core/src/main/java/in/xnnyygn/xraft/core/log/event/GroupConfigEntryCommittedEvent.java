package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryCommittedEvent extends AbstractEntryEvent<GroupConfigEntry> {

    public GroupConfigEntryCommittedEvent(GroupConfigEntry entry) {
        super(entry);
    }

}
