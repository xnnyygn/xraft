package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.NodeBuilder;
import in.xnnyygn.xraft.core.node.NodeGroup;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        NodeGroup nodeGroup = new NodeGroup();
        Node nodeA = new NodeBuilder("A", nodeGroup).build();
        Node nodeB = new NodeBuilder("B", nodeGroup).build();
        Node nodeC = new NodeBuilder("C", nodeGroup).build();

        Service service = new Service();
        Server serverA = new Server(nodeA, service, 3333);
        Server serverB = new Server(nodeB, service, 3334);
        Server serverC = new Server(nodeC, service, 3335);
        try {
            serverA.start();
            serverB.start();
            serverC.start();
            System.in.read();
        } finally {
            serverA.stop();
            serverB.stop();
            serverC.stop();
        }
    }
}
