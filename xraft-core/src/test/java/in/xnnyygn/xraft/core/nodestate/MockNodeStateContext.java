package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.*;

public class MockNodeStateContext implements NodeStateContext {

    private ElectionTimeoutScheduler electionTimeoutScheduler = new NullElectionTimeoutScheduler();
    private NodeId selfNodeId;
    private int nodeCount;
    private ReplicationStateTracker replicationStateTracker;
    private Log log;
    private Connector connector;
    private AbstractNodeState nodeState;

    @Override
    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    @Override
    public int getNodeCount() {
        return this.nodeCount;
    }

    @Override
    public ReplicationStateTracker createReplicationStateTracker() {
        return this.replicationStateTracker;
    }

    @Override
    public void changeToNodeState(AbstractNodeState newNodeState) {
        this.nodeState = newNodeState;
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

    public void setReplicationStateTracker(ReplicationStateTracker replicationStateTracker) {
        this.replicationStateTracker = replicationStateTracker;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public AbstractNodeState getNodeState() {
        return nodeState;
    }

}
