package in.xnnyygn.xraft.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import in.xnnyygn.xraft.actor.ElectionActor;
import in.xnnyygn.xraft.actor.RpcActor;
import in.xnnyygn.xraft.actor.TimeoutActor;

public class RaftNodeBuilder {

    private String actionSystemName = "raft";
    private String nodeId;
    private RaftNodeGroup group;
    private RaftNodeSave nodeState = new RaftNodeSave();

    public RaftNodeBuilder withActorSystemName(String actorSystemName) {
        this.actionSystemName = actorSystemName;
        return this;
    }

    public RaftNodeBuilder withGroup(RaftNodeGroup group) {
        this.group = group;
        return this;
    }

    public RaftNodeBuilder withNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public Server build() {
        if (this.group == null) {
            throw new IllegalArgumentException("group is required");
        }

        if (this.nodeId == null) {
            throw new IllegalArgumentException("nodeId required");
        }

        RaftNodeId selfNodeId = new RaftNodeId(this.nodeId);
        ActorSystem actorSystem = ActorSystem.create(this.actionSystemName);
        ActorRef electionActor = actorSystem.actorOf(Props.create(ElectionActor.class, this.group, selfNodeId, this.nodeState), "election");
        // TODO remove timeout actor
        ActorRef timeoutActor = actorSystem.actorOf(Props.create(TimeoutActor.class, selfNodeId), "timeout");
        ActorRef rpcActor = actorSystem.actorOf(Props.create(RpcActor.class, this.group, selfNodeId), "rpc");
        Server node = new Server(selfNodeId, actorSystem);
        group.addNode(node);
        return node;
    }

}
