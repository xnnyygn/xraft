package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.log.TaskReference;
import in.xnnyygn.xraft.core.noderole.NodeRoleListener;
import in.xnnyygn.xraft.core.noderole.RoleNameAndLeaderId;
import in.xnnyygn.xraft.core.noderole.RoleStateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeImpl implements Node {

    private static final Logger logger = LoggerFactory.getLogger(NodeImpl.class);
    private final Controller controller;

    public NodeImpl(NodeContext context) {
        this.controller = new Controller(context);
    }

    @Override
    public void start() {
        logger.info("node {}, start", this.controller.getSelfNodeId());
        this.controller.start();
    }

    @Override
    public RoleNameAndLeaderId getRoleNameAndLeaderId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RoleStateSnapshot getRoleState() {
        return this.controller.getRoleState();
    }

    @Override
    public void registerStateMachine(StateMachine stateMachine) {
        this.controller.registerStateMachine(stateMachine);
    }

    @Override
    public void addNodeRoleListener(NodeRoleListener listener) {
        this.controller.addNodeRoleListener(listener);
    }

    @Override
    public void appendLog(byte[] commandBytes) {
        // assert this.controller.getRoleState().getRole() == RoleName.LEADER;
        controller.appendLog(commandBytes);
    }

    @Override
    public TaskReference addServer(NodeEndpoint newNodeEndpoint) {
        return controller.addServer(newNodeEndpoint);
    }

    @Override
    public TaskReference removeServer(NodeId id) {
        return controller.removeServer(id);
    }

    @Override
    public void stop() throws InterruptedException {
        logger.info("node {}, stop", this.controller.getSelfNodeId());
        this.controller.stop();
    }

}
