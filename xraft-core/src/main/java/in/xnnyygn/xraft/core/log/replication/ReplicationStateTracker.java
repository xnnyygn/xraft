package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Collection;

public interface ReplicationStateTracker {

    class NodeMatchIndex implements Comparable<NodeMatchIndex> {

        private final NodeId nodeId;
        private final int matchIndex;

        NodeMatchIndex(ReplicationState state) {
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
        public int compareTo(NodeMatchIndex o) {
            return -Integer.compare(o.matchIndex, this.matchIndex);
        }

        @Override
        public String toString() {
            return "<" + this.nodeId + ", " + this.matchIndex + ">";
        }

    }

    ReplicationState get(NodeId id);

    int getMajorMatchIndex();

    Collection<NodeId> listPeer();

}
