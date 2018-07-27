package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.noderole.NodeRoleListener;
import in.xnnyygn.xraft.core.noderole.RoleStateSnapshot;
import in.xnnyygn.xraft.core.service.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class Node {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final Controller controller;

    public Node(NodeContext context) {
        this.controller = new Controller(context);
    }

    public void start() {
        logger.info("node {}, start", this.controller.getSelfNodeId());
        this.controller.start();
    }

    public RoleStateSnapshot getRoleState() {
        return this.controller.getRoleState();
    }

    public void registerStateMachine(StateMachine stateMachine) {
        this.controller.registerStateMachine(stateMachine);
    }

    public void addNodeRoleListener(NodeRoleListener listener) {
        this.controller.addNodeRoleListener(listener);
    }

    public void appendLog(byte[] commandBytes) {
        // assert this.controller.getRoleState().getRole() == RoleName.LEADER;
        this.controller.appendLog(commandBytes);
    }

    public void addServer(NodeConfig newNodeConfig) {

    }

    public void stop() throws InterruptedException {
        logger.info("node {}, stop", this.controller.getSelfNodeId());
        this.controller.stop();
    }

}
