package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;

public class NodeStateException extends RuntimeException {

    private final NodeRole role;
    private final NodeId leaderId;

    public NodeStateException(NodeRole role, NodeId leaderId) {
        this.role = role;
        this.leaderId = leaderId;
    }

    @Override
    public String getMessage() {
        return "unexpected node state, role " + this.role + ", leader id " + this.leaderId;
    }

    public NodeRole getRole() {
        return role;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public boolean isLeaderIdPresent() {
        return this.leaderId != null;
    }

}
