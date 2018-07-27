package in.xnnyygn.xraft.core.log.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ReplicationStateTrackerUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationStateTrackerUtils.class);

    public static int getMajorMatchIndex(Collection<ReplicationState> replicationStates) {
        if (replicationStates.isEmpty()) {
            throw new IllegalArgumentException("no replication state");
        }

        List<ReplicationStateTracker.NodeMatchIndex> matchIndices = new ArrayList<>();
        for (ReplicationState state : replicationStates) {
            matchIndices.add(new ReplicationStateTracker.NodeMatchIndex(state));
        }
        Collections.sort(matchIndices);
        logger.debug("match indices {}", matchIndices);

        // 5 nodes, A B C D E => C, index 5 / 2 => 2
        // 6 nodes, A B C D E F => D, index 6 / 2 => 3
        return matchIndices.get(matchIndices.size() / 2).getMatchIndex();
    }

}
