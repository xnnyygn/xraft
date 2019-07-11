package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReadIndexTask {

    private static final Logger logger = LoggerFactory.getLogger(ReadIndexTask.class);
    private final int commitIndex;
    private final Map<NodeId, Integer> matchIndices;
    private final StateMachine stateMachine;
    private final String requestId;

    public ReadIndexTask(int commitIndex, @Nonnull Set<NodeId> followerIds, @Nonnull StateMachine stateMachine, String requestId) {
        this.commitIndex = commitIndex;
        this.stateMachine = stateMachine;
        this.requestId = requestId;

        matchIndices = new HashMap<>();
        for (NodeId nodeId : followerIds) {
            matchIndices.put(nodeId, 0);
        }
    }

    public String getRequestId() {
        return requestId;
    }

    // node thread
    public boolean updateMatchIndex(@Nonnull NodeId nodeId, int matchIndex) {
        logger.debug("update match index, node id {}, match index {}", nodeId, matchIndex);
        if (!matchIndices.containsKey(nodeId)) {
            // TODO, call state machine group changed
            return false;
        }
        matchIndices.put(nodeId, matchIndex);
        long countOfFollowerReachingCommitIndex = matchIndices.values().stream().filter(i -> i >= commitIndex).count();
        // if commit
        if (countOfFollowerReachingCommitIndex + 1 > (matchIndices.size() + 1) / 2) {
            stateMachine.onReadIndexReached(requestId, commitIndex);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ReadIndexTask{" +
                "commitIndex=" + commitIndex +
                ", matchIndices=" + matchIndices +
                ", requestId='" + requestId + '\'' +
                '}';
    }
}
