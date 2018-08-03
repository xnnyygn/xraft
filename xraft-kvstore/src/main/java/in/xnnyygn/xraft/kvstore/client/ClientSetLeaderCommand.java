package in.xnnyygn.xraft.kvstore.client;

import in.xnnyygn.xraft.core.node.NodeId;

public class ClientSetLeaderCommand implements Command {

    @Override
    public String getName() {
        return "client-set-leader";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("usage: " + getName() + " <node-id>");
        }

        NodeId nodeId = new NodeId(arguments);
        try {
            context.setClientLeader(nodeId);
            System.out.println(nodeId);
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
        }
    }

}
