package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.NodeBuilder;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.service.ServerRouter;

public class Launcher {

    public static void main(String[] args) throws Exception {
        NodeGroup nodeGroup = new NodeGroup();

        Node node1 = new NodeBuilder("A", nodeGroup).build();
        Node node2 = new NodeBuilder("B", nodeGroup).build();
        Node node3 = new NodeBuilder("C", nodeGroup).build();

        ServerRouter router = new ServerRouter();
        router.add(node1.getId(), new EmbeddedChannel(new Service(node1)));
        router.add(node2.getId(), new EmbeddedChannel(new Service(node2)));
        router.add(node3.getId(), new EmbeddedChannel(new Service(node3)));
        Client client = new Client(router);

        Thread clientThread;
        try {
            nodeGroup.startAll();

            clientThread = new Thread(() -> {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                }
                client.set("x", 1);
            });
            clientThread.start();

            System.in.read();
        } finally {
            nodeGroup.stopAll();
        }
    }
}
