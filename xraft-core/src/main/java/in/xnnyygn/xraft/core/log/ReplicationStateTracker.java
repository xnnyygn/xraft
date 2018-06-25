package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;

import java.util.*;

public class ReplicationStateTracker {

    private final Map<NodeId, ReplicationState> replicationStateMap;

    public ReplicationStateTracker(Collection<NodeId> nodeIds, int nextIndex) {
        this.replicationStateMap = new HashMap<>();

        for (NodeId id : nodeIds) {
            this.replicationStateMap.put(id, new ReplicationState(id, nextIndex));
        }
    }

    public ReplicationState get(NodeId id) {
        ReplicationState state = this.replicationStateMap.get(id);
        if (state == null) {
            throw new IllegalStateException("replication state of node " + id + " not found");
        }
        return state;
    }

    public int getMajorMatchIndex() {
        Integer[] matchIndexes = this.replicationStateMap.values().stream().map(
                ReplicationState::getMatchIndex
        ).sorted().toArray(Integer[]::new);
        // 5 nodes, 4 values, A B C D => C, index 4 / 2 => 2
        // 6 nodes, 5 values, A B C D E => C, index 5 / 2 => 2
        return matchIndexes[this.replicationStateMap.size() / 2];
    }

    public Collection<NodeId> listNodeNotReplicating() {
        List<NodeId> nodeIds = new ArrayList<>();
        for (ReplicationState state : this.replicationStateMap.values()) {
            if (!state.isReplicating()) {
                nodeIds.add(state.getNodeId());
            }
        }
        return nodeIds;
    }

}
