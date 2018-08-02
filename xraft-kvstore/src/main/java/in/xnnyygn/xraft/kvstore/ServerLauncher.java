package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.*;

import java.util.HashSet;
import java.util.Set;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: <command>");
            return;
        }

        String command = args[0];
        Server server;
        if ("group-node".equals(command)) {
            Set<NodeConfig> nodeConfigs = new HashSet<>();
            nodeConfigs.add(new NodeConfig("A", "localhost", 2333));
            nodeConfigs.add(new NodeConfig("B", "localhost", 2334));
            nodeConfigs.add(new NodeConfig("C", "localhost", 2335));
            NodeGroup nodeGroup = new NodeGroup(nodeConfigs);
            NodeId nodeId = new NodeId(args[1]);
            Node node = new NodeBuilder(nodeId, nodeGroup).build();
            server = new Server(node, nodeGroup.getEndpoint(nodeId).getPort() + 1000);
        } else if ("new-node".equals(command)) {
            NodeConfig newNodeConfig = new NodeConfig(args[1], "localhost", Integer.parseInt(args[2]));
            NodeGroup nodeGroup = new NodeGroup(newNodeConfig);
            Node node = new NodeBuilder(newNodeConfig.getId(), nodeGroup).setStandby(true).build();
            server = new Server(node, newNodeConfig.getPort() + 1000);
        } else {
            throw new IllegalArgumentException("unknown command " + command);
        }
        try {
            server.start();
            System.in.read();
        } finally {
            server.stop();
        }
    }
}
