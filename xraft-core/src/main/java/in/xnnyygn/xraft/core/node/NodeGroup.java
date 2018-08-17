package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.replication.PeerReplicatingState;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
import in.xnnyygn.xraft.core.log.replication.SelfReplicatingState;
import in.xnnyygn.xraft.core.rpc.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.Collectors;

@NotThreadSafe
public class NodeGroup {

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private Map<NodeId, NodeState> stateMap;

    public NodeGroup(NodeEndpoint endpoint) {
        this(Collections.singleton(endpoint));
    }

    public NodeGroup(Collection<NodeEndpoint> endpoints) {
        this.stateMap = buildStateMap(endpoints);
    }

    private Map<NodeId, NodeState> buildStateMap(Collection<NodeEndpoint> endpoints) {
        Map<NodeId, NodeState> map = new HashMap<>();
        for (NodeEndpoint endpoint : endpoints) {
            map.put(endpoint.getId(), new NodeState(endpoint));
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("endpoints is empty");
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

    private NodeState findState(NodeId id) {
        NodeState state = stateMap.get(id);
        if (state == null) {
            throw new IllegalArgumentException("no such node " + id);
        }
        return state;
    }

    public NodeState getState(NodeId id) {
        return stateMap.get(id);
    }

    @Deprecated
    public Address findAddress(NodeId id) {
        return findState(id).getAddress();
    }

    public ReplicatingState findReplicationState(NodeId id) {
        return findState(id).getReplicatingState();
    }

    public boolean isMemberOfMajor(NodeId id) {
        NodeState state = stateMap.get(id);
        return state != null && state.isMemberOfMajor();
    }

    public NodeEndpoint findEndpoint(NodeId id) {
        return findState(id).getEndpoint();
    }

    public void upgrade(NodeId id) {
        logger.info("upgrade node {}", id);
        NodeState state = findState(id);
        state.setMemberOfMajor(true);

        // replication state of new node -> peer
        PeerReplicatingState newReplicationState = new PeerReplicatingState(state.getReplicatingState());
        state.setReplicatingState(newReplicationState);
    }

    public void downgrade(NodeId id) {
        logger.info("downgrade node {}", id);
        NodeState state = findState(id);
        state.setMemberOfMajor(false);
        state.setRemoving(true);
    }

    public void resetReplicationStates(NodeId selfId, Log log) {
        for (NodeState state : stateMap.values()) {
            if (state.getId().equals(selfId)) {
                state.setReplicatingState(new SelfReplicatingState(selfId, log));
            } else {
                state.setReplicatingState(new PeerReplicatingState(state.getId(), log.getNextIndex()));
            }
        }
    }

    @Deprecated
    public void resetReplicationStates(int nextLogIndex) {
        for (NodeState state : stateMap.values()) {
            state.setReplicatingState(new PeerReplicatingState(state.getId(), nextLogIndex));
        }
    }

    public int getMatchIndexOfMajor() {
        List<NodeMatchIndex> matchIndices = new ArrayList<>();
        for (NodeState state : stateMap.values()) {
            if (state.isMemberOfMajor()) {
                matchIndices.add(new NodeMatchIndex(state.getReplicatingState()));
            }
        }
        if (matchIndices.isEmpty()) {
            throw new IllegalArgumentException("no replication state");
        }
        Collections.sort(matchIndices);
        logger.debug("match indices {}", matchIndices);

        // 5 nodes, A B C D E => C, index 5 / 2 => 2
        // 6 nodes, A B C D E F => D, index 6 / 2 => 3
        return matchIndices.get(matchIndices.size() / 2).getMatchIndex();
    }

    // TODO add test
    public Collection<NodeGroup.NodeState> getReplicationTargets() {
        return stateMap.values().stream()
                .filter(NodeState::isReplicationTarget)
                .collect(Collectors.toList());
    }

    public NodeState addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex, boolean memberOfMajor) {
        logger.info("add node {} to group, endpoint {}", endpoint.getId(), endpoint);
        NodeState state = new NodeState(endpoint, new PeerReplicatingState(endpoint.getId(), nextIndex, matchIndex), memberOfMajor);
        stateMap.put(endpoint.getId(), state);
        return state;
    }

    public void removeNode(NodeId id) {
        logger.info("node {} removed", id);
        stateMap.remove(id);
    }

    public void updateNodes(Set<NodeEndpoint> endpoints) {
        logger.info("update nodes to {}", endpoints);
        stateMap = buildStateMap(endpoints);
    }

    // TODO optimize
    public Set<NodeEndpoint> getNodeEndpointsOfMajor() {
        return stateMap.values().stream()
                .filter(NodeState::isMemberOfMajor)
                .map(NodeState::getEndpoint)
                .collect(Collectors.toSet());
    }

    public boolean isUniqueNode(NodeId id) {
        return stateMap.size() == 1 && stateMap.containsKey(id);
    }

    public static class NodeState {

        private final NodeEndpoint endpoint;
        private ReplicatingState replicatingState;
        private boolean memberOfMajor;
        private boolean removing = false;

        NodeState(NodeEndpoint endpoint) {
            this(endpoint, true);
        }

        NodeState(NodeEndpoint endpoint, boolean memberOfMajor) {
            this.endpoint = endpoint;
            this.memberOfMajor = memberOfMajor;
        }

        NodeState(NodeEndpoint endpoint, ReplicatingState replicatingState, boolean memberOfMajor) {
            this.endpoint = endpoint;
            this.replicatingState = replicatingState;
            this.memberOfMajor = memberOfMajor;
        }

        NodeEndpoint getEndpoint() {
            return endpoint;
        }

        NodeId getId() {
            return endpoint.getId();
        }

        Address getAddress() {
            return endpoint.getAddress();
        }

        void setReplicatingState(ReplicatingState replicatingState) {
            this.replicatingState = replicatingState;
        }

        public ReplicatingState getReplicatingState() {
            if (replicatingState == null) {
                throw new IllegalStateException("replication state not set");
            }
            return replicatingState;
        }

        public boolean isMemberOfMajor() {
            return memberOfMajor;
        }

        void setMemberOfMajor(boolean memberOfMajor) {
            this.memberOfMajor = memberOfMajor;
        }

        public boolean isRemoving() {
            return removing;
        }

        public void setRemoving(boolean removing) {
            this.removing = removing;
        }

        public int getNextIndex() {
            return getReplicatingState().getNextIndex();
        }

        public void startReplicating() {
            getReplicatingState().startReplicating();
        }

        public boolean isReplicationTarget() {
            return getReplicatingState().isReplicationTarget();
        }

        // TODO add test
        public boolean shouldReplicate(long minReplicationInterval) {
            ReplicatingState replicatingState = getReplicatingState();
            return !replicatingState.isReplicating() ||
                    System.currentTimeMillis() - replicatingState.getLastReplicatedAt() > minReplicationInterval;
        }

        @Override
        public String toString() {
            return "NodeState{" +
                    "endpoint=" + endpoint +
                    ", memberOfMajor=" + memberOfMajor +
                    ", removing=" + removing +
                    ", replicatingState=" + replicatingState +
                    '}';
        }

    }

    private static class NodeMatchIndex implements Comparable<NodeMatchIndex> {

        private final NodeId nodeId;
        private final int matchIndex;

        NodeMatchIndex(ReplicatingState state) {
            this(state.getNodeId(), state.getMatchIndex());
        }

        NodeMatchIndex(NodeId nodeId, int matchIndex) {
            this.nodeId = nodeId;
            this.matchIndex = matchIndex;
        }

        int getMatchIndex() {
            return matchIndex;
        }

        @Override
        public int compareTo(@Nonnull NodeMatchIndex o) {
            return -Integer.compare(o.matchIndex, this.matchIndex);
        }

        @Override
        public String toString() {
            return "<" + nodeId + ", " + matchIndex + ">";
        }

    }

}
