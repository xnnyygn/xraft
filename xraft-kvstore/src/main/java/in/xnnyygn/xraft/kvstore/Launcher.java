package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.server.Server;
import in.xnnyygn.xraft.core.server.ServerBuilder;
import in.xnnyygn.xraft.core.server.ServerGroup;
import in.xnnyygn.xraft.kvstore.client.Client;
import in.xnnyygn.xraft.kvstore.client.EmbeddedChannel;
import in.xnnyygn.xraft.kvstore.client.Selector;

public class Launcher {

    public static void main(String[] args) throws Exception {
        ServerGroup serverGroup = new ServerGroup();

        Server server1 = new ServerBuilder("A", serverGroup).build();
        Server server2 = new ServerBuilder("B", serverGroup).build();
        Server server3 = new ServerBuilder("C", serverGroup).build();

        Selector selector = new Selector();
        selector.add(server1.getId(), new EmbeddedChannel(new Service(server1)));
        selector.add(server2.getId(), new EmbeddedChannel(new Service(server2)));
        selector.add(server3.getId(), new EmbeddedChannel(new Service(server3)));
        Client client = new Client(selector);

        Thread clientThread;
        try {
            serverGroup.startAll();

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
            serverGroup.stopAll();
        }
    }
}
