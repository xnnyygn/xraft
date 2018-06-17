package in.xnnyygn.xraft;

import in.xnnyygn.xraft.server.Server;
import in.xnnyygn.xraft.server.ServerBuilder;
import in.xnnyygn.xraft.server.ServerGroup;

public class Launcher {

    public static void main(String[] args) throws Exception {
        ServerGroup serverGroup = new ServerGroup();
        Server server1 = new ServerBuilder().withServerId("A").withActorSystemName("raft1").withGroup(serverGroup).build();
        Server server2 = new ServerBuilder().withServerId("B").withActorSystemName("raft2").withGroup(serverGroup).build();
        Server server3 = new ServerBuilder().withServerId("C").withActorSystemName("raft3").withGroup(serverGroup).build();
        try {
            serverGroup.startAll();
            System.in.read();
        } finally {
            serverGroup.stopAll();
        }
    }
}
