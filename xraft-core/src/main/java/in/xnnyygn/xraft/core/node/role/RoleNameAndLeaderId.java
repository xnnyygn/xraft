package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.concurrent.Immutable;

@Immutable
public class RoleNameAndLeaderId {

    private final RoleName roleName;
    private final NodeId leaderId;

    public RoleNameAndLeaderId(RoleName roleName, NodeId leaderId) {
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
