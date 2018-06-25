package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.node.NodeId;

public interface NodeStateContext extends ElectionTimeoutScheduler {

    NodeId getSelfNodeId();

    int getNodeCount();

    ReplicationStateTracker createReplicationStateTracker();

    void changeToNodeState(AbstractNodeState newNodeState);

    LogReplicationTask scheduleLogReplicationTask();

    Log getLog();

    Connector getConnector();

}
