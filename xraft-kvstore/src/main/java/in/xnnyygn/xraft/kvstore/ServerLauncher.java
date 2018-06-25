package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.*;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        NodeGroup nodeGroup = new NodeGroup();
        Node nodeA = new NodeBuilder("A", nodeGroup).build();
        Node nodeB = new NodeBuilder("B", nodeGroup).build();
        Node nodeC = new NodeBuilder("C", nodeGroup).build();

        Server serverA = new Server(nodeA, new Service(), 3333);
        Server serverB = new Server(nodeB, new Service(), 3334);
        Server serverC = new Server(nodeC, new Service(), 3335);
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
