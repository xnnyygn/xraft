package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nonnull;
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
        public int compareTo(@Nonnull NodeMatchIndex o) {
            return -Integer.compare(o.matchIndex, this.matchIndex);
        }

        @Override
        public String toString() {
            return "<" + this.nodeId + ", " + this.matchIndex + ">";
        }

    }

    ReplicationState get(NodeId id);

    int getMajorMatchIndex();

    Collection<ReplicationState> replicationTargets();

    void add(ReplicationState replicationState);

}
