package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.node.NodeId;

public interface NodeStateContext extends ElectionTimeoutScheduler {

    NodeId getSelfNodeId();

    int getNodeCount();

    void setNodeState(AbstractNodeState nodeState);

    LogReplicationTask scheduleLogReplicationTask();

    Connector getRpcConnector();

}
