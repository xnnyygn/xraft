package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.replication.GeneralReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.*;

public class MockNodeRoleContext implements NodeRoleContext {

    private ElectionTimeoutScheduler electionTimeoutScheduler = new NullElectionTimeoutScheduler();
    private NodeId selfNodeId;
    private int nodeCount;
    private GeneralReplicationStateTracker replicationStateTracker;
    private Log log;
    private Connector connector;
    private AbstractNodeRole nodeState;

    @Override
    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    @Override
    public int getNodeCountForVoting() {
        return this.nodeCount;
    }

    @Override
    public GeneralReplicationStateTracker createReplicationStateTracker() {
        return this.replicationStateTracker;
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

    public void setSelfNodeId(NodeId selfNodeId) {
        this.selfNodeId = selfNodeId;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public void setReplicationStateTracker(GeneralReplicationStateTracker replicationStateTracker) {
        this.replicationStateTracker = replicationStateTracker;
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
