package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.replication.GeneralReplicationStateTracker;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.node.NodeId;

public interface NodeRoleContext extends ElectionTimeoutScheduler {

    NodeId getSelfNodeId();

    int getNodeCountForVoting();

    GeneralReplicationStateTracker createReplicationStateTracker();

    void changeToNodeRole(AbstractNodeRole newNodeRole);

    LogReplicationTask scheduleLogReplicationTask();

    Log getLog();

    Connector getConnector();

}
