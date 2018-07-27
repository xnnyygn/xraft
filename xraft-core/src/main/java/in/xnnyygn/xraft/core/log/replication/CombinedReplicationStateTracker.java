package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import java.util.*;

public class CombinedReplicationStateTracker implements ReplicationStateTracker {

    private final Map<NodeId, ReplicationState> oldReplicationStateMap;
    private final Map<NodeId, ReplicationState> newReplicationStateMap;
    private final Set<NodeId> peerIds;

    public CombinedReplicationStateTracker(GeneralReplicationStateTracker oldTracker, Collection<NodeId> newNodeIds, int nextIndex) {
        this.oldReplicationStateMap = oldTracker.getReplicationStateMap();
        this.newReplicationStateMap = buildNewReplicationStateMap(this.oldReplicationStateMap, newNodeIds, nextIndex);
        this.peerIds = filterPeerIds(this.oldReplicationStateMap, newNodeIds);
    }

    private static Set<NodeId> filterPeerIds(Map<NodeId, ReplicationState> oldReplicationStateMap, Collection<NodeId> newNodeIds) {
        Set<NodeId> nodeIds = new HashSet<>(oldReplicationStateMap.keySet());
        nodeIds.addAll(newNodeIds);

        Optional<ReplicationState> replicationState = oldReplicationStateMap.values().stream().filter(s ->
                s instanceof SelfReplicationState).findFirst();
        replicationState.ifPresent(s -> nodeIds.remove(s.getNodeId()));
        return nodeIds;
    }

    private static Map<NodeId, ReplicationState> buildNewReplicationStateMap(
            Map<NodeId, ReplicationState> oldReplicationStateMap, Collection<NodeId> newNodeIds, int nextIndex) {
        Map<NodeId, ReplicationState> map = new HashMap<>();
        for (NodeId nodeId : newNodeIds) {
            ReplicationState state = oldReplicationStateMap.get(nodeId);
            if (state != null) {
                map.put(nodeId, state);
            } else {
                map.put(nodeId, new GeneralReplicationState(nodeId, nextIndex));
            }
        }
        return map;
    }

    @Override
    public ReplicationState get(NodeId id) {
        ReplicationState state = this.oldReplicationStateMap.get(id);
        if (state != null) return state;

        state = this.newReplicationStateMap.get(id);
        if (state == null) {
            throw new IllegalStateException("no replication state for node " + id);
        }
        return state;
    }

    @Override
    public int getMajorMatchIndex() {
        return Math.min(
                ReplicationStateTrackerUtils.getMajorMatchIndex(this.oldReplicationStateMap.values()),
                ReplicationStateTrackerUtils.getMajorMatchIndex(this.newReplicationStateMap.values())
        );
    }

    @Override
    public Collection<NodeId> listPeer() {
        return this.peerIds;
    }

    public Map<NodeId, ReplicationState> getNewReplicationStateMap() {
        return this.newReplicationStateMap;
    }

}
