package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.replication.PeerReplicatingState;
import in.xnnyygn.xraft.core.node.replication.SelfReplicatingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Node group.
 */
// TODO fix test
@NotThreadSafe
public class NodeGroup {

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private Map<NodeId, GroupMember> memberMap;

    /**
     * Create group with single member(standalone).
     *
     * @param endpoint endpoint
     */
    public NodeGroup(NodeEndpoint endpoint) {
        this(Collections.singleton(endpoint));
    }

    /**
     * Create group.
     *
     * @param endpoints endpoints
     */
    public NodeGroup(Collection<NodeEndpoint> endpoints) {
        this.memberMap = buildMemberMap(endpoints);
    }

    /**
     * Build member map from endpoints.
     *
     * @param endpoints endpoints
     * @return member map
     * @throws IllegalArgumentException if endpoints is empty
     */
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
     * <p>For election.</p>
     *
     * @return count
     * @see GroupMember#isMajor()
     */
    int getCountOfMajor() {
        return (int) memberMap.values().stream().filter(GroupMember::isMajor).count();
    }

    /**
     * Find member by id.
     * <p>Throw exception if member not found.</p>
     *
     * @param id id
     * @return member, never be {@code null}
     * @throws IllegalArgumentException if member not found
     */
    @Nonnull
    GroupMember findMember(NodeId id) {
        GroupMember member = getMember(id);
        if (member == null) {
            throw new IllegalArgumentException("no such node " + id);
        }
        return member;
    }

    /**
     * Get member by id.
     *
     * @param id id
     * @return member, maybe {@code null}
     */
    @Nullable
    GroupMember getMember(NodeId id) {
        return memberMap.get(id);
    }

    /**
     * Check if node is major member.
     *
     * @param id id
     * @return true if member exists and member is major, otherwise false
     */
    boolean isMemberOfMajor(NodeId id) {
        GroupMember member = memberMap.get(id);
        return member != null && member.isMajor();
    }

    /**
     * Upgrade member to major member.
     *
     * @param id id
     * @throws IllegalArgumentException if member not found
     * @see #findMember(NodeId)
     */
    void upgrade(NodeId id) {
        logger.info("upgrade node {}", id);
        findMember(id).setMajor(true);
    }

    /**
     * Downgrade member(set major to {@code false}).
     *
     * @param id id
     * @throws IllegalArgumentException if member not found
     */
    void downgrade(NodeId id) {
        logger.info("downgrade node {}", id);
        GroupMember member = findMember(id);
        member.setMajor(false);
        member.setRemoving();
    }

    /**
     * Remove member.
     *
     * @param id id
     */
    void removeNode(NodeId id) {
        logger.info("node {} removed", id);
        memberMap.remove(id);
    }

    /**
     * Reset replicating state.
     *
     * @param selfId self id
     * @param log    log
     */
    void resetReplicatingStates(NodeId selfId, Log log) {
        for (GroupMember member : memberMap.values()) {
            if (member.getId().equals(selfId)) {
                member.setReplicatingState(new SelfReplicatingState(selfId, log));
            } else {
                member.setReplicatingState(new PeerReplicatingState(member.getId(), log.getNextIndex()));
            }
        }
    }

    /**
     * Get match index of major members.
     *
     * @return match index
     */
    // TODO check test
    int getMatchIndexOfMajor() {
        List<NodeMatchIndex> matchIndices = new ArrayList<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor()) {
                matchIndices.add(new NodeMatchIndex(member.getId(), member.getMatchIndex()));
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

    /**
     * List replication target.
     * <p>Self is not replication target.</p>
     *
     * @return replication targets.
     */
    // TODO add test
    Collection<GroupMember> listReplicationTarget() {
        return memberMap.values().stream()
                .filter(GroupMember::isReplicationTarget)
                .collect(Collectors.toList());
    }

    /**
     * Add member to group.
     *
     * @param endpoint   endpoint
     * @param nextIndex  next index
     * @param matchIndex match index
     * @param major      major
     * @return added member
     */
    GroupMember addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex, boolean major) {
        logger.info("add node {} to group, endpoint {}", endpoint.getId(), endpoint);
        PeerReplicatingState replicatingState = new PeerReplicatingState(endpoint.getId(), nextIndex, matchIndex);
        GroupMember member = new GroupMember(endpoint, replicatingState, major);
        memberMap.put(endpoint.getId(), member);
        return member;
    }

    /**
     * Update member list.
     * <p>All replicating state will be dropped.</p>
     *
     * @param endpoints endpoints
     */
    void updateNodes(Set<NodeEndpoint> endpoints) {
        logger.info("update nodes to {}", endpoints);
        memberMap = buildMemberMap(endpoints);
    }

    /**
     * List endpoint of major members.
     *
     * @return endpoints
     */
    Set<NodeEndpoint> listEndpointOfMajor() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor()) {
                endpoints.add(member.getEndpoint());
            }
        }
        return endpoints;
    }

    /**
     * List endpoint of major members except self.
     *
     * @param selfId self id
     * @return endpoints
     */
    Set<NodeEndpoint> listEndpointOfMajorExcept(NodeId selfId) {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor() && !member.getId().equals(selfId)) {
                endpoints.add(member.getEndpoint());
            }
        }
        return endpoints;
    }

    /**
     * Check if member is unique one in group, in other word, check if standalone mode.
     *
     * @param id id
     * @return true if only one member and the id of member equals to specified id, otherwise false
     */
    boolean isUniqueNode(NodeId id) {
        return memberMap.size() == 1 && memberMap.containsKey(id);
    }

    /**
     * Node match index.
     *
     * @see NodeGroup#getMatchIndexOfMajor()
     */
    private static class NodeMatchIndex implements Comparable<NodeMatchIndex> {

        private final NodeId nodeId;
        private final int matchIndex;

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
