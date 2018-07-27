package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class GeneralReplicationStateTracker implements ReplicationStateTracker {

    private static final Logger logger = LoggerFactory.getLogger(GeneralReplicationStateTracker.class);
    private final Map<NodeId, ReplicationState> replicationStateMap;

    public GeneralReplicationStateTracker(Collection<NodeId> nodeIds, int nextIndex) {
        this(buildReplicationStateMap(nodeIds, nextIndex));
    }

    private static Map<NodeId, ReplicationState> buildReplicationStateMap(Collection<NodeId> nodeIds, int nextIndex) {
        Map<NodeId, ReplicationState> map = new HashMap<>();
        for (NodeId id : nodeIds) {
            map.put(id, new GeneralReplicationState(id, nextIndex));
        }
        return map;
    }

    public GeneralReplicationStateTracker(Collection<NodeId> peerIds, int nextIndex, ReplicationState selfState) {
        Map<NodeId, ReplicationState> map = buildReplicationStateMap(peerIds, nextIndex);
        map.put(selfState.getNodeId(), selfState);
        this.replicationStateMap = map;
    }

    public GeneralReplicationStateTracker(Map<NodeId, ReplicationState> replicationStateMap) {
        this.replicationStateMap = replicationStateMap;
    }

    @Override
    public ReplicationState get(NodeId id) {
        ReplicationState state = this.replicationStateMap.get(id);
        if (state == null) {
            throw new IllegalStateException("replication state of node " + id + " not found");
        }
        return state;
    }

    @Override
    public int getMajorMatchIndex() {
        return ReplicationStateTrackerUtils.getMajorMatchIndex(this.replicationStateMap.values());
    }

    @Override
    public Collection<NodeId> listPeer() {
        return this.replicationStateMap.values().stream()
                .filter(s -> !(s instanceof SelfReplicationState))
                .map(ReplicationState::getNodeId)
                .collect(Collectors.toList());
    }

    public Map<NodeId, ReplicationState> getReplicationStateMap() {
        return this.replicationStateMap;
    }

    public void applyNodeConfigs(Set<NodeConfig> nodeConfigs, int nextIndex, NodeId selfNodeId, ReplicationState selfState) {
        Set<NodeId> nodeIds = new HashSet<>();
        for (NodeConfig nodeConfig : nodeConfigs) {
            NodeId nodeId = nodeConfig.getId();
            if (!this.replicationStateMap.containsKey(nodeId)) {
                // new node
                if (nodeId.equals(selfNodeId)) {
                    this.replicationStateMap.put(nodeId, selfState);
                } else {
                    this.replicationStateMap.put(nodeId, new GeneralReplicationState(nodeId, nextIndex, false));
                }
            }
            nodeIds.add(nodeId);
        }
        for (NodeId nodeId : this.replicationStateMap.keySet()) {
            if (!nodeIds.contains(nodeId)) {
                // node to remove
                this.replicationStateMap.remove(nodeId);
            }
        }
    }

}
