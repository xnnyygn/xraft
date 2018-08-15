package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;

public abstract class AbstractNodeRole {

    private final RoleName name;
    protected final int term;

    AbstractNodeRole(RoleName name, int term) {
        this.name = name;
        this.term = term;
    }

    public RoleName getName() {
        return name;
    }

    public int getTerm() {
        return term;
    }

    public RoleNameAndLeaderId getNameAndLeaderId(NodeId selfId) {
        return new RoleNameAndLeaderId(name, getLeaderId(selfId));
    }

    public abstract NodeId getLeaderId(NodeId selfId);

    public abstract void cancelTimeoutOrTask();

    public abstract RoleState getState();

}
