package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;

public interface NodeRoleContext extends ElectionTimeoutScheduler {

    NodeId getSelfNodeId();

    NodeGroup getNodeGroup();

    void resetReplicationStates();

    void upgradeNode(NodeId id);

    void removeNode(NodeId id);

    void changeToNodeRole(AbstractNodeRole newNodeRole);

    LogReplicationTask scheduleLogReplicationTask();

    Log getLog();

    Connector getConnector();

    boolean standbyMode();

}
