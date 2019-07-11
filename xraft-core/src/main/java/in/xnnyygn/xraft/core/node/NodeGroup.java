package in.xnnyygn.xraft.core.node;

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
@NotThreadSafe
class NodeGroup {

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private final NodeId selfId;
    private Map<NodeId, GroupMember> memberMap;

    /**
     * Create group with single member(standalone).
     *
     * @param endpoint endpoint
     */
    NodeGroup(NodeEndpoint endpoint) {
        this(Collections.singleton(endpoint), endpoint.getId());
    }

    /**
     * Create group.
     *
     * @param endpoints endpoints
     * @param selfId    self id
     */
    NodeGroup(Collection<NodeEndpoint> endpoints, NodeId selfId) {
        this.memberMap = buildMemberMap(endpoints);
        this.selfId = selfId;
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
     * Find self.
     *
     * @return self
     */
    @Nonnull
    GroupMember findSelf() {
        return findMember(selfId);
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

//    /**
//     * Downgrade member(set major to {@code false}).
//     *
//     * @param id id
//     * @throws IllegalArgumentException if member not found
//     */
//    void downgrade(NodeId id) {
//        logger.info("downgrade node {}", id);
//        GroupMember member = findMember(id);
//        member.setMajor(false);
//    }

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
     * @param nextLogIndex next log index
     */
    void resetReplicatingStates(int nextLogIndex) {
        for (GroupMember member : memberMap.values()) {
            if (!member.idEquals(selfId)) {
                member.setReplicatingState(new ReplicatingState(nextLogIndex));
            }
        }
    }

    /**
     * Get match index of major members.
     * <p>
     * To get major match index in group, sort match indices and get the middle one.
     * </p>
     * TODO add doc
     *
     * @return match index
     */
    int getMatchIndexOfMajor() {
        List<NodeMatchIndex> matchIndices = new ArrayList<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor()) {
                if (member.idEquals(selfId)) {
                    matchIndices.add(new NodeMatchIndex(selfId));
                } else {
                    matchIndices.add(new NodeMatchIndex(member.getId(), member.getMatchIndex()));
                }
            }
        }
        int count = matchIndices.size();
        if (count == 0) {
            throw new IllegalStateException("no major node");
        }
        if (count == 1 && matchIndices.get(0).nodeId == selfId) {
            throw new IllegalStateException("standalone");
        }
        Collections.sort(matchIndices);
        logger.debug("match indices {}", matchIndices);
        int index = (count % 2 == 0 ? count / 2 - 1 : count / 2);
        return matchIndices.get(index).getMatchIndex();
    }

    /**
     * List replication target.
     * <p>Self is not replication target.</p>
     *
     * @return replication targets.
     */
    Collection<GroupMember> listReplicationTarget() {
        return memberMap.values().stream().filter(m -> !m.idEquals(selfId)).collect(Collectors.toList());
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
        logger.info("add node {} to group", endpoint.getId());
        ReplicatingState replicatingState = new ReplicatingState(nextIndex, matchIndex);
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
        memberMap = buildMemberMap(endpoints);
        logger.info("group config changed -> {}", memberMap.keySet());
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
     * @return endpoints except self
     */
    Set<NodeEndpoint> listEndpointOfMajorExceptSelf() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            if (member.isMajor() && !member.idEquals(selfId)) {
                endpoints.add(member.getEndpoint());
            }
        }
        return endpoints;
    }

    Set<NodeId> listIdOfMajorExceptSelf() {
        Set<NodeId> ids = new HashSet<>();
        for(GroupMember member : memberMap.values()) {
            if(member.isMajor() && !member.idEquals(selfId)) {
                ids.add(member.getId());
            }
        }
        return ids;
    }

    /**
     * Check if member is unique one in group, in other word, check if standalone mode.
     *
     * @return true if only one member and the id of member equals to specified id, otherwise false
     */
    boolean isStandalone() {
        return memberMap.size() == 1 && memberMap.containsKey(selfId);
    }

    /**
     * Node match index.
     *
     * @see NodeGroup#getMatchIndexOfMajor()
     */
    private static class NodeMatchIndex implements Comparable<NodeMatchIndex> {

        private final NodeId nodeId;
        private final int matchIndex;
        private final boolean leader;

        NodeMatchIndex(NodeId nodeId) {
            this(nodeId, Integer.MAX_VALUE, true);
        }

        NodeMatchIndex(NodeId nodeId, int matchIndex) {
            this(nodeId, matchIndex, false);
        }

        private NodeMatchIndex(NodeId nodeId, int matchIndex, boolean leader) {
            this.nodeId = nodeId;
            this.matchIndex = matchIndex;
            this.leader = leader;
        }

        int getMatchIndex() {
            return matchIndex;
        }

        @Override
        public int compareTo(@Nonnull NodeMatchIndex o) {
            return Integer.compare(this.matchIndex, o.matchIndex);
        }

        @Override
        public String toString() {
            return "<" + nodeId + ", " + (leader ? "L" : matchIndex) + ">";
        }

    }

}
