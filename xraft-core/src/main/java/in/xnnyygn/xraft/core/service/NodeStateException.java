package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.server.ServerId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;

public class NodeStateException extends RuntimeException {

    private final NodeRole role;
    private final ServerId leaderId;

    public NodeStateException(NodeRole role, ServerId leaderId) {
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

    public ServerId getLeaderId() {
        return leaderId;
    }

    public boolean isLeaderIdPresent() {
        return this.leaderId != null;
    }

}
