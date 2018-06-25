package in.xnnyygn.xraft.core.node;

public class Node2Launcher {

    public static void main(String[] args) throws Exception {
        NodeGroup group = new NodeGroup();

        Node2 nodeA = new NodeBuilder2("A", group).build();
        Node2 nodeB = new NodeBuilder2("B", group).build();
        Node2 nodeC = new NodeBuilder2("C", group).build();

        try {
            System.out.println("Start nodes");
            nodeA.start();
            nodeB.start();
            nodeC.start();
            System.in.read();
        } finally {
            nodeA.stop();
            nodeB.stop();
            nodeC.stop();
        }
    }

}
