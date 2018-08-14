package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryFromLeaderAppendEvent extends AbstractEntryEvent<GroupConfigEntry> {

    public GroupConfigEntryFromLeaderAppendEvent(GroupConfigEntry entry) {
        super(entry);
    }

}
