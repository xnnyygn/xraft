package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryBatchRemovedEvent {

    private final GroupConfigEntry firstRemovedEntry;

    public GroupConfigEntryBatchRemovedEvent(GroupConfigEntry firstRemovedEntry) {
        this.firstRemovedEntry = firstRemovedEntry;
    }

    public GroupConfigEntry getFirstRemovedEntry() {
        return firstRemovedEntry;
    }

}
