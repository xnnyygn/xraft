package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.*;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        NodeGroup nodeGroup = new NodeGroup();
        Node2 nodeA = new NodeBuilder2("A", nodeGroup).build();
        Node2 nodeB = new NodeBuilder2("B", nodeGroup).build();
        Node2 nodeC = new NodeBuilder2("C", nodeGroup).build();

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
