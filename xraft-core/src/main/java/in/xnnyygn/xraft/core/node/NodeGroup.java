package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.replication.PeerReplicatingState;
import in.xnnyygn.xraft.core.node.replication.ReplicatingState;
import in.xnnyygn.xraft.core.node.replication.SelfReplicatingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.Collectors;

// TODO fix test
@NotThreadSafe
public class NodeGroup {

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private Map<NodeId, GroupMember> memberMap;

    public NodeGroup(NodeEndpoint endpoint) {
        this(Collections.singleton(endpoint));
    }

    public NodeGroup(Collection<NodeEndpoint> endpoints) {
        this.memberMap = buildMemberMap(endpoints);
    }

    private Map<NodeId, GroupMember> buildMemberMap(Collection<NodeEndpoint> endpoints) {
        Map<NodeId, GroupMember> map = new HashMap<>();
        for (NodeEndpoint endpoint : endpoints) {
            map.put(endpoint.getId(), new GroupMember(endpoint));
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
    int getCountOfMajor() {
        return (int) memberMap.values().stream().filter(GroupMember::isMajor).count();
    }

    GroupMember findMember(NodeId id) {
        GroupMember member = getMember(id);
        if (member == null) {
            throw new IllegalArgumentException("no such node " + id);
        }
        return member;
    }

    GroupMember getMember(NodeId id) {
        return memberMap.get(id);
    }

    boolean isMemberOfMajor(NodeId id) {
        GroupMember member = memberMap.get(id);
        return member != null && member.isMajor();
    }

    void upgrade(NodeId id) {
        logger.info("upgrade node {}", id);
        findMember(id).setMajor(true);
    }

    void downgrade(NodeId id) {
        logger.info("downgrade node {}", id);
        GroupMember member = findMember(id);
        member.setMajor(false);
        member.setRemoving(true);
    }

    void removeNode(NodeId id) {
        logger.info("node {} removed", id);
        memberMap.remove(id);
    }

    void resetReplicationStates(NodeId selfId, Log log) {
        for (GroupMember member : memberMap.values()) {
            if (member.getId().equals(selfId)) {
                member.setReplicatingState(new SelfReplicatingState(selfId, log));
            } else {
                member.setReplicatingState(new PeerReplicatingState(member.getId(), log.getNextIndex()));
            }
        }
    }

    // TODO check test
    int getMatchIndexOfMajor() {
        List<NodeMatchIndex> matchIndices = new ArrayList<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor()) {
                matchIndices.add(new NodeMatchIndex(member.getReplicatingState()));
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
    Collection<GroupMember> listReplicationTarget() {
        return memberMap.values().stream()
                .filter(GroupMember::isReplicationTarget)
                .collect(Collectors.toList());
    }

    GroupMember addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex, boolean major) {
        logger.info("add node {} to group, endpoint {}", endpoint.getId(), endpoint);
        PeerReplicatingState replicatingState = new PeerReplicatingState(endpoint.getId(), nextIndex, matchIndex);
        GroupMember member = new GroupMember(endpoint, replicatingState, major);
        memberMap.put(endpoint.getId(), member);
        return member;
    }

    void updateNodes(Set<NodeEndpoint> endpoints) {
        logger.info("update nodes to {}", endpoints);
        memberMap = buildMemberMap(endpoints);
    }

    Set<NodeEndpoint> listEndpointOfMajor() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor()) {
                endpoints.add(member.getEndpoint());
            }
        }
        return endpoints;
    }

    Set<NodeEndpoint> listEndpointOfMajorExclude(NodeId selfId) {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor() && !member.getId().equals(selfId)) {
                endpoints.add(member.getEndpoint());
            }
        }
        return endpoints;
    }

    boolean isUniqueNode(NodeId id) {
        return memberMap.size() == 1 && memberMap.containsKey(id);
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
