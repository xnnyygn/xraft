package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultReplicationStateTracker implements ReplicationStateTracker {

    private final Map<NodeId, ReplicationState> replicationStateMap;

    /**
     * Create with node ids and next index.
     *
     * @param nodeIds   node ids
     * @param nextIndex next index
     */
    public DefaultReplicationStateTracker(Collection<NodeId> nodeIds, int nextIndex) {
        this(buildReplicationStateMap(nodeIds, nextIndex));
    }

    private static Map<NodeId, ReplicationState> buildReplicationStateMap(Collection<NodeId> nodeIds, int nextIndex) {
        Map<NodeId, ReplicationState> map = new HashMap<>();
        for (NodeId id : nodeIds) {
            map.put(id, new PeerReplicationState(id, nextIndex));
        }
        return map;
    }

    /**
     * Create with peer ids, next index and self state.
     *
     * @param peerIds   peer ids
     * @param nextIndex next index
     * @param selfState self state
     */
    public DefaultReplicationStateTracker(Collection<NodeId> peerIds, int nextIndex, ReplicationState selfState) {
        Map<NodeId, ReplicationState> map = buildReplicationStateMap(peerIds, nextIndex);
        map.put(selfState.getNodeId(), selfState);
        this.replicationStateMap = map;
    }

    /**
     * Create with replication state map.
     *
     * @param replicationStateMap replication state map
     */
    public DefaultReplicationStateTracker(Map<NodeId, ReplicationState> replicationStateMap) {
        this.replicationStateMap = replicationStateMap;
    }

    @Override
    public ReplicationState get(NodeId id) {
        ReplicationState state = replicationStateMap.get(id);
        if (state == null) {
            throw new IllegalStateException("replication state of node " + id + " not found");
        }
        return state;
    }

    @Override
    public int getMajorMatchIndex() {
        return ReplicationStateTrackerUtils.getMajorMatchIndex(replicationStateMap.values());
    }

    @Override
    public Collection<ReplicationState> replicationTargets() {
        return replicationStateMap.values().stream().filter(ReplicationState::isReplicationTarget).collect(Collectors.toList());
    }

    @Override
    public void add(ReplicationState replicationState) {
        NodeId nodeId = replicationState.getNodeId();
        ReplicationState oldReplicationState = replicationStateMap.get(nodeId);
        if (oldReplicationState != null) {
            throw new IllegalStateException("replication state of " + nodeId + " exists");
        }
        replicationStateMap.put(nodeId, replicationState);
    }

}
