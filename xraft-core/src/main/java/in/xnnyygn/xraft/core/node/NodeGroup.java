package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.replication.*;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class NodeGroup {

    private static class NodeState {

        private final NodeConfig config;
        private ReplicationState replicationState;
        private boolean memberOfMajor;

        NodeState(NodeConfig config) {
            this(config, true);
        }

        NodeState(NodeConfig config, boolean memberOfMajor) {
            this.config = config;
            this.memberOfMajor = memberOfMajor;
        }

        NodeState(NodeConfig config, ReplicationState replicationState, boolean memberOfMajor) {
            this.config = config;
            this.replicationState = replicationState;
            this.memberOfMajor = memberOfMajor;
        }

        NodeConfig getConfig() {
            return config;
        }

        NodeId getId() {
            return config.getId();
        }

        Endpoint getEndpoint() {
            return config.getEndpoint();
        }

        void setReplicationState(ReplicationState replicationState) {
            this.replicationState = replicationState;
        }

        ReplicationState getReplicationState() {
            if (replicationState == null) {
                throw new IllegalStateException("replication state not set");
            }
            return replicationState;
        }

        boolean isMemberOfMajor() {
            return memberOfMajor;
        }

        void setMemberOfMajor(boolean memberOfMajor) {
            this.memberOfMajor = memberOfMajor;
        }

        @Override
        public String toString() {
            return "NodeState{" +
                    "config=" + config +
                    ", memberOfMajor=" + memberOfMajor +
                    ", replicationState=" + replicationState +
                    '}';
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private Map<NodeId, NodeState> stateMap;

    public NodeGroup(NodeConfig config) {
        this(Collections.singleton(config));
    }

    public NodeGroup(Set<NodeConfig> configs) {
        this.stateMap = buildStateMap(configs);
    }

    private Map<NodeId, NodeState> buildStateMap(Set<NodeConfig> configs) {
        Map<NodeId, NodeState> map = new HashMap<>();
        for (NodeConfig config : configs) {
            map.put(config.getId(), new NodeState(config));
        }
        return map;
    }

    /**
     * Get count of major.
     * For election.
     *
     * @return count
     */
    public int getCountOfMajor() {
        return (int) stateMap.values().stream().filter(NodeState::isMemberOfMajor).count();
    }

    public Endpoint getEndpoint(NodeId id) {
        return getState(id).getEndpoint();
    }

    private NodeState getState(NodeId id) {
        NodeState state = stateMap.get(id);
        if (state == null) {
            throw new IllegalStateException("no such node " + id);
        }
        return state;
    }

    public NodeConfig getConfig(NodeId id) {
        return getState(id).getConfig();
    }

    public boolean isMemberOfMajor(NodeId id) {
        return getState(id).isMemberOfMajor();
    }

    public void upgrade(NodeId id) {
        logger.info("upgrade node {}", id);
        NodeState state = getState(id);
        state.setMemberOfMajor(true);

        // replication state of new node -> peer
        PeerReplicationState newReplicationState = new PeerReplicationState(state.getReplicationState());
        state.setReplicationState(newReplicationState);
    }

    public void resetReplicationStates(NodeId selfId, Log log) {
        for (NodeState state : stateMap.values()) {
            if (state.getId().equals(selfId)) {
                state.setReplicationState(new SelfReplicationState(selfId, log));
            } else {
                state.setReplicationState(new PeerReplicationState(state.getId(), log.getNextIndex()));
            }
        }
    }

    public void resetReplicationStates(int nextLogIndex) {
        for (NodeState state : stateMap.values()) {
            state.setReplicationState(new PeerReplicationState(state.getId(), nextLogIndex));
        }
    }

    public ReplicationState getReplicationState(NodeId id) {
        return getState(id).getReplicationState();
    }

    public int getMatchIndexOfMajor() {
        List<ReplicationState> replicationStates = stateMap.values().stream()
                .filter(NodeState::isMemberOfMajor)
                .map(NodeState::getReplicationState)
                .collect(Collectors.toList());
        return ReplicationStateTrackerUtils.getMajorMatchIndex(replicationStates);
    }

    public Collection<ReplicationState> getReplicationTargets() {
        return stateMap.values().stream()
                .map(NodeState::getReplicationState)
                .filter(ReplicationState::isReplicationTarget)
                .collect(Collectors.toList());
    }

    public void addNode(NodeConfig config, int nextLogIndex, boolean memberOfMajor) {
        logger.info("add node {} to group, member of group {}", config, memberOfMajor);
        stateMap.put(config.getId(), new NodeState(
                config, new NewNodeReplicationState(config.getId(), nextLogIndex), memberOfMajor
        ));
    }

    public void removeNode(NodeId id) {
        logger.info("remove node {}", id);
        stateMap.remove(id);
    }

    public void updateNodes(Set<NodeConfig> configs) {
        logger.info("update nodes to {}", configs);
        stateMap = buildStateMap(configs);
    }

    public Set<NodeConfig> getNodeConfigsOfMajor() {
        return stateMap.values().stream()
                .filter(NodeState::isMemberOfMajor)
                .map(NodeState::getConfig)
                .collect(Collectors.toSet());
    }

    public boolean isUniqueNode(NodeId id) {
        return stateMap.size() == 1 && stateMap.containsKey(id);
    }

}
