package in.xnnyygn.xraft;

import in.xnnyygn.xraft.server.Server;
import in.xnnyygn.xraft.server.ServerBuilder;
import in.xnnyygn.xraft.server.RaftNodeGroup;

public class Launcher {

    public static void main(String[] args) throws Exception {
        RaftNodeGroup nodeGroup = new RaftNodeGroup();
        Server node1 = new ServerBuilder().withNodeId("A").withActorSystemName("raft1").withGroup(nodeGroup).build();
        Server node2 = new ServerBuilder().withNodeId("B").withActorSystemName("raft2").withGroup(nodeGroup).build();
        Server node3 = new ServerBuilder().withNodeId("C").withActorSystemName("raft3").withGroup(nodeGroup).build();
        try {
            nodeGroup.startAll();
            System.in.read();
        } finally {
            nodeGroup.stopAll();
        }
    }
}
