package in.xnnyygn.xraft.core.node.config;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.NodeBuilder;

/**
 * Node configuration.
 * <p>
 * Node configuration should not change after initialization. e.g {@link NodeBuilder}.
 * </p>
 */
public class NodeConfig {

    /**
     * Minimum election timeout
     */
    private int minElectionTimeout = 3000;

    /**
     * Maximum election timeout
     */
    private int maxElectionTimeout = 4000;

    /**
     * Delay for first log replication after becoming leader
     */
    private int logReplicationDelay = 0;

    /**
     * Interval for log replication task.
     * More specifically, interval for heartbeat rpc.
     * Append entries rpc may be sent less than this interval.
     * e.g after receiving append entries result from followers.
     */
    private int logReplicationInterval = 1000;

    /**
     * Read timeout to receive response from follower.
     * If no response received from follower, resend log replication rpc.
     */
    private int logReplicationReadTimeout = 900;

    /**
     * Max entries to send when replicate log to followers
     */
    private int maxReplicationEntries = Log.ALL_ENTRIES;

    /**
     * Max entries to send when replicate log to new node
     */
    private int maxReplicationEntriesForNewNode = Log.ALL_ENTRIES;

    /**
     * Data length in install snapshot rpc.
     */
    private int snapshotDataLength = 1024;

    /**
     * Worker thread count in nio connector.
     */
    private int nioWorkerThreads = 0;

    /**
     * Max round for new node to catch up.
     */
    private int newNodeMaxRound = 10;

    /**
     * Read timeout to receive response from new node.
     * Default to election timeout.
     */
    private int newNodeReadTimeout = 3000;

    /**
     * Timeout for new node to make progress.
     * If new node cannot make progress after this timeout, new node cannot be added and reply TIMEOUT.
     * Default to election timeout
     */
    private int newNodeAdvanceTimeout = 3000;

    /**
     * Timeout to wait for previous group config change to complete.
     * Default is {@code 0}, forever.
     */
    private int previousGroupConfigChangeTimeout = 0;

    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    public void setMinElectionTimeout(int minElectionTimeout) {
        this.minElectionTimeout = minElectionTimeout;
    }

    public int getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    public void setMaxElectionTimeout(int maxElectionTimeout) {
        this.maxElectionTimeout = maxElectionTimeout;
    }

    public int getLogReplicationDelay() {
        return logReplicationDelay;
    }

    public void setLogReplicationDelay(int logReplicationDelay) {
        this.logReplicationDelay = logReplicationDelay;
    }

    public int getLogReplicationInterval() {
        return logReplicationInterval;
    }

    public void setLogReplicationInterval(int logReplicationInterval) {
        this.logReplicationInterval = logReplicationInterval;
    }

    public int getLogReplicationReadTimeout() {
        return logReplicationReadTimeout;
    }

    public void setLogReplicationReadTimeout(int logReplicationReadTimeout) {
        this.logReplicationReadTimeout = logReplicationReadTimeout;
    }

    public int getMaxReplicationEntries() {
        return maxReplicationEntries;
    }

    public void setMaxReplicationEntries(int maxReplicationEntries) {
        this.maxReplicationEntries = maxReplicationEntries;
    }

    public int getMaxReplicationEntriesForNewNode() {
        return maxReplicationEntriesForNewNode;
    }

    public void setMaxReplicationEntriesForNewNode(int maxReplicationEntriesForNewNode) {
        this.maxReplicationEntriesForNewNode = maxReplicationEntriesForNewNode;
    }

    public int getSnapshotDataLength() {
        return snapshotDataLength;
    }

    public void setSnapshotDataLength(int snapshotDataLength) {
        this.snapshotDataLength = snapshotDataLength;
    }

    public int getNioWorkerThreads() {
        return nioWorkerThreads;
    }

    public void setNioWorkerThreads(int nioWorkerThreads) {
        this.nioWorkerThreads = nioWorkerThreads;
    }

    public int getNewNodeMaxRound() {
        return newNodeMaxRound;
    }

    public void setNewNodeMaxRound(int newNodeMaxRound) {
        this.newNodeMaxRound = newNodeMaxRound;
    }

    public int getNewNodeReadTimeout() {
        return newNodeReadTimeout;
    }

    public void setNewNodeReadTimeout(int newNodeReadTimeout) {
        this.newNodeReadTimeout = newNodeReadTimeout;
    }

    public int getPreviousGroupConfigChangeTimeout() {
        return previousGroupConfigChangeTimeout;
    }

    public void setPreviousGroupConfigChangeTimeout(int previousGroupConfigChangeTimeout) {
        this.previousGroupConfigChangeTimeout = previousGroupConfigChangeTimeout;
    }

    public int getNewNodeAdvanceTimeout() {
        return newNodeAdvanceTimeout;
    }

    public void setNewNodeAdvanceTimeout(int newNodeAdvanceTimeout) {
        this.newNodeAdvanceTimeout = newNodeAdvanceTimeout;
    }

}
