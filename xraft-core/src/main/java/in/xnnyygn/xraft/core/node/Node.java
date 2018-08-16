package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.node.task.GroupConfigChangeTaskReference;

public interface Node {

    void registerStateMachine(StateMachine stateMachine);

    RoleNameAndLeaderId getRoleNameAndLeaderId();

    RoleState getRoleState();

    void addNodeRoleListener(NodeRoleListener listener);

    void start();

    void appendLog(byte[] commandBytes);

    GroupConfigChangeTaskReference addNode(NodeEndpoint newNodeEndpoint);

    GroupConfigChangeTaskReference removeNode(NodeId id);

    void stop() throws InterruptedException;

}
