package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.scheduler.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.scheduler.LogReplicationTask;
import in.xnnyygn.xraft.messages.RaftMessage;
import in.xnnyygn.xraft.node.RaftNodeId;

// TODO rename to a better one
public interface ServerStateContext extends ElectionTimeoutScheduler {

    RaftNodeId getSelfNodeId();

    int getNodeCount();

    void setNodeState(AbstractServerState nodeState);

    LogReplicationTask scheduleLogReplicationTask();

    void sendRpcOrResultMessage(RaftMessage message);

}
