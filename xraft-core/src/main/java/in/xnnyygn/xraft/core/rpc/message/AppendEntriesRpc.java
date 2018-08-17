package in.xnnyygn.xraft.core.rpc.message;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.node.NodeId;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class AppendEntriesRpc implements Serializable {

    private String messageId;
    private int term;
    private NodeId leaderId;
    private int prevLogIndex = 0;
    private int prevLogTerm;
    private List<Entry> entries = Collections.emptyList();
    private int leaderCommit;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public int getLastEntryIndex() {
        return this.entries.isEmpty() ? this.prevLogIndex : this.entries.get(this.entries.size() - 1).getIndex();
    }

    @Override
    public String toString() {
        return "AppendEntriesRpc{" +
                "messageId='" + messageId +
                "', entries.size=" + entries.size() +
                ", leaderCommit=" + leaderCommit +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", term=" + term +
                '}';
    }
}
