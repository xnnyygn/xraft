package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.log.TaskReference;
import in.xnnyygn.xraft.core.noderole.NodeRoleListener;
import in.xnnyygn.xraft.core.noderole.RoleNameAndLeaderId;
import in.xnnyygn.xraft.core.noderole.RoleStateSnapshot;

public interface Node {

    void registerStateMachine(StateMachine stateMachine);

    RoleNameAndLeaderId getRoleNameAndLeaderId();

    @Deprecated
    RoleStateSnapshot getRoleState();

    void addNodeRoleListener(NodeRoleListener listener);

    void start();

    void appendLog(byte[] commandBytes);

    TaskReference addServer(NodeEndpoint newNodeEndpoint);

    TaskReference removeServer(NodeId id);

    void stop() throws InterruptedException;

}
