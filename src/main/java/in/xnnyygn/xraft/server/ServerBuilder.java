package in.xnnyygn.xraft.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import in.xnnyygn.xraft.actor.ElectionActor;
import in.xnnyygn.xraft.actor.RpcActor;

public class ServerBuilder {

    private String actionSystemName = "raft";
    private String serverId;
    private ServerGroup group;
    private ServerStore serverState = new ServerStore();

    public ServerBuilder withActorSystemName(String actorSystemName) {
        this.actionSystemName = actorSystemName;
        return this;
    }

    public ServerBuilder withGroup(ServerGroup group) {
        this.group = group;
        return this;
    }

    public ServerBuilder withServerId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    public Server build() {
        if (this.group == null) {
            throw new IllegalArgumentException("group is required");
        }

        if (this.serverId == null) {
            throw new IllegalArgumentException("serverId required");
        }

        ServerId selfServerId = new ServerId(this.serverId);
        ActorSystem actorSystem = ActorSystem.create(this.actionSystemName);
        ActorRef electionActor = actorSystem.actorOf(Props.create(ElectionActor.class, this.group, selfServerId, this.serverState), "election");
        ActorRef rpcActor = actorSystem.actorOf(Props.create(RpcActor.class, this.group, selfServerId), "rpc");
        Server server = new Server(selfServerId, actorSystem);
        group.addServer(server);
        return server;
    }

}
