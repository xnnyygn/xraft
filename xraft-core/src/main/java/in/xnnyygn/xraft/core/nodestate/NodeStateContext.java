package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.rpc.Router;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.server.ServerId;

public interface NodeStateContext extends ElectionTimeoutScheduler {

    ServerId getSelfNodeId();

    int getNodeCount();

    void setNodeState(AbstractNodeState nodeState);

    LogReplicationTask scheduleLogReplicationTask();

    Router getRpcRouter();

}
