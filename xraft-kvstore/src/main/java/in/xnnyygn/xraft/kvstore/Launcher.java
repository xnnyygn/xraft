package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.server.Server;
import in.xnnyygn.xraft.core.server.ServerBuilder;
import in.xnnyygn.xraft.core.server.ServerGroup;
import in.xnnyygn.xraft.core.service.ServerRouter;

public class Launcher {

    public static void main(String[] args) throws Exception {
        ServerGroup serverGroup = new ServerGroup();

        Server server1 = new ServerBuilder("A", serverGroup).build();
        Server server2 = new ServerBuilder("B", serverGroup).build();
        Server server3 = new ServerBuilder("C", serverGroup).build();

        ServerRouter router = new ServerRouter();
        router.add(server1.getId(), new EmbeddedChannel(new Service(server1)));
        router.add(server2.getId(), new EmbeddedChannel(new Service(server2)));
        router.add(server3.getId(), new EmbeddedChannel(new Service(server3)));
        Client client = new Client(router);

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
