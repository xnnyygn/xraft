package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.noderole.RoleName;

public class NotLeaderException extends RuntimeException {

    private final RoleName roleName;
    private final NodeId leaderId;

    public NotLeaderException(RoleName roleName, NodeId leaderId) {
        super("not leader");
        this.roleName = roleName;
        this.leaderId = leaderId;
    }

    public RoleName getRoleName() {
        return roleName;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

}
