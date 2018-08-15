package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.Log;

// TODO rename to NodeConfig
public class NodeConfig2 {

    private int minElectionTimeout = 3000;
    private int maxElectionTimeout = 4000;
    private int logReplicationDelay = 0;
    private int logReplicationInterval = 1000;
    private int minReplicationInterval = 900;
    private int maxReplicationEntries = Log.ALL_ENTRIES;
    private int maxReplicationEntriesForNewNode = Log.ALL_ENTRIES;
    private int snapshotDataLength = 1024;
    private int nioWorkerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private int newNodeMaxRound = 10;
    private int newNodeTimeout = 3000;
    private int previousGroupConfigChangeTimeout = 1000;

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

    public int getMinReplicationInterval() {
        return minReplicationInterval;
    }

    public void setMinReplicationInterval(int minReplicationInterval) {
        this.minReplicationInterval = minReplicationInterval;
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

    public int getNewNodeTimeout() {
        return newNodeTimeout;
    }

    public void setNewNodeTimeout(int newNodeTimeout) {
        this.newNodeTimeout = newNodeTimeout;
    }

    public int getPreviousGroupConfigChangeTimeout() {
        return previousGroupConfigChangeTimeout;
    }

    public void setPreviousGroupConfigChangeTimeout(int previousGroupConfigChangeTimeout) {
        this.previousGroupConfigChangeTimeout = previousGroupConfigChangeTimeout;
    }

}
