package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.node.role.RoleState;

public interface NodeRoleListener {

    void nodeRoleChanged(RoleState roleState);

}
