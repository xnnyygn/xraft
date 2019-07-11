package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.util.Set;

public class AppendEntriesState {

    public static final AppendEntriesState FAILED = new AppendEntriesState(false, null);
    public static final AppendEntriesState SUCCESS = new AppendEntriesState(true, null);

    private final boolean success;
    private Set<NodeEndpoint> latestGroup;

    public AppendEntriesState(Set<NodeEndpoint> latestGroup) {
        this(true, latestGroup);
    }

    private AppendEntriesState(boolean success, Set<NodeEndpoint> latestGroup) {
        this.success = success;
        this.latestGroup = latestGroup;
    }

    public boolean isSuccess() {
        return success;
    }

    public Set<NodeEndpoint> getLatestGroup() {
        return latestGroup;
    }

    public boolean hasGroup() {
        return latestGroup != null;
    }
}
