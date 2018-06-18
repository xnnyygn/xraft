package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.server.Server;
import in.xnnyygn.xraft.server.ServerBuilder;
import in.xnnyygn.xraft.server.ServerGroup;

public class Launcher {

    public static void main(String[] args) throws Exception {
        ServerGroup serverGroup = new ServerGroup();
        Server server1 = new ServerBuilder("A", serverGroup).build();
        Server server2 = new ServerBuilder("B", serverGroup).build();
        Server server3 = new ServerBuilder("C", serverGroup).build();
        try {
            serverGroup.startAll();
            System.in.read();
        } finally {
            serverGroup.stopAll();
        }
    }
}
