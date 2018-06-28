package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.*;
import in.xnnyygn.xraft.core.rpc.socket.SocketEndpoint;

import java.util.HashMap;
import java.util.Map;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
//        NodeGroup nodeGroup = new NodeGroup();
//        Node nodeA = new NodeBuilder("A", nodeGroup).build();
//        Node nodeB = new NodeBuilder("B", nodeGroup).build();
//        Node nodeC = new NodeBuilder("C", nodeGroup).build();
//
//        Server serverA = new Server(nodeA, new Service(), 3333);
//        Server serverB = new Server(nodeB, new Service(), 3334);
//        Server serverC = new Server(nodeC, new Service(), 3335);
//        try {
//            serverA.start();
//            serverB.start();
//            serverC.start();
//            System.in.read();
//        } finally {
//            serverA.stop();
//            serverB.stop();
//            serverC.stop();
//        }

        Map<String, SocketEndpoint> nodeMap = new HashMap<>();
        nodeMap.put("A", new SocketEndpoint("localhost", 2333));
        nodeMap.put("B", new SocketEndpoint("localhost", 2334));
        nodeMap.put("C", new SocketEndpoint("localhost", 2335));

        String nodeId = args[0];

        NodeGroup nodeGroup = new NodeGroup();
        SocketEndpoint endpoint = nodeMap.remove(nodeId);
        Node node = new NodeBuilder(nodeId, nodeGroup).setSocketEndpoint(endpoint).build();

        for (String remoteNodeId : nodeMap.keySet()) {
            nodeGroup.add(new RemoteNode(new NodeId(remoteNodeId), nodeMap.get(remoteNodeId)));
        }

        Server server = new Server(node, new Service(), endpoint.getPort() + 1000);
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
