package in.xnnyygn.xraft.kvstore.message;

import in.xnnyygn.xraft.core.node.NodeId;

public class Redirect {

    private final String leaderId;

    public Redirect(NodeId leaderId) {
        this(leaderId != null ? leaderId.getValue() : null);
    }

    public Redirect(String leaderId) {
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "Redirect{" + "leaderId=" + leaderId + '}';
    }

}
