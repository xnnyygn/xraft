package in.xnnyygn.xraft;

import in.xnnyygn.xraft.node.RaftNode;
import in.xnnyygn.xraft.node.RaftNodeBuilder;
import in.xnnyygn.xraft.node.RaftNodeGroup;

public class Launcher {

    public static void main(String[] args) throws Exception {
        RaftNodeGroup nodeGroup = new RaftNodeGroup();
        RaftNode node1 = new RaftNodeBuilder().withNodeId("A").withActorSystemName("raft1").withGroup(nodeGroup).build();
        RaftNode node2 = new RaftNodeBuilder().withNodeId("B").withActorSystemName("raft2").withGroup(nodeGroup).build();
        RaftNode node3 = new RaftNodeBuilder().withNodeId("C").withActorSystemName("raft3").withGroup(nodeGroup).build();
        try {
            nodeGroup.startAll();
            System.in.read();
        } finally {
            nodeGroup.stopAll();
        }
    }
}
