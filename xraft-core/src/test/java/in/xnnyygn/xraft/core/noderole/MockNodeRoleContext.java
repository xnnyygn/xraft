package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.*;

public class MockNodeRoleContext implements NodeRoleContext {

    private ElectionTimeoutScheduler electionTimeoutScheduler = new NullElectionTimeoutScheduler();
    private NodeId selfNodeId;
    private Log log;
    private NodeGroup nodeGroup;
    private Connector connector;
    private AbstractNodeRole nodeState;

    @Override
    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    @Override
    public NodeGroup getNodeGroup() {
        return nodeGroup;
    }

    @Override
    public void resetReplicationStates() {
    }

    @Override
    public void upgradeNode(NodeId id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNode(NodeId id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeToNodeRole(AbstractNodeRole newNodeRole) {
        this.nodeState = newNodeRole;
    }

    @Override
    public LogReplicationTask scheduleLogReplicationTask() {
        return new LogReplicationTask(new NullScheduledFuture());
    }

    @Override
    public Log getLog() {
        return this.log;
    }

    @Override
    public Connector getConnector() {
        return this.connector;
    }

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return electionTimeoutScheduler.scheduleElectionTimeout();
    }

    @Override
    public boolean standbyMode() {
        return false;
    }

    public void setSelfNodeId(NodeId selfNodeId) {
        this.selfNodeId = selfNodeId;
    }

    public void setNodeGroup(NodeGroup nodeGroup) {
        this.nodeGroup = nodeGroup;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public AbstractNodeRole getNodeState() {
        return nodeState;
    }

}
