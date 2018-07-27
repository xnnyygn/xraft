package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.*;

import java.util.HashSet;
import java.util.Set;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        Set<NodeConfig> nodeConfigs = new HashSet<>();
        nodeConfigs.add(new NodeConfig("A", "localhost", 2333));
        nodeConfigs.add(new NodeConfig("B", "localhost", 2334));
        nodeConfigs.add(new NodeConfig("C", "localhost", 2335));

        NodeId nodeId = new NodeId(args[0]);
        NodeGroup nodeGroup = new NodeGroup(nodeConfigs);
        Node node = new NodeBuilder(nodeId, nodeGroup).build();
        Server server = new Server(node, nodeGroup.find(nodeId).getPort() + 1000);
        try {
            server.start();
//            node.start();
            System.in.read();
        } finally {
            server.stop();
//            node.stop();
        }
    }
}
