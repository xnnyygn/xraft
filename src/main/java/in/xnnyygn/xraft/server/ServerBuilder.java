package in.xnnyygn.xraft.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import in.xnnyygn.xraft.actor.ElectionActor;
import in.xnnyygn.xraft.actor.ElectionActorRpcChannelListener;
import in.xnnyygn.xraft.rpc.EmbeddedChannel;
import in.xnnyygn.xraft.rpc.Router;

public class ServerBuilder {

    private String actionSystemName = "raft";
    private String serverId;
    private ServerGroup serverGroup;
    private ServerStore serverState = new ServerStore();

    public ServerBuilder withActorSystemName(String actorSystemName) {
        this.actionSystemName = actorSystemName;
        return this;
    }

    public ServerBuilder withGroup(ServerGroup group) {
        this.serverGroup = group;
        return this;
    }

    public ServerBuilder withServerId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    public Server build() {
        if (this.serverGroup == null) {
            throw new IllegalArgumentException("group is required");
        }

        if (this.serverId == null) {
            throw new IllegalArgumentException("serverId required");
        }

        ServerId selfServerId = new ServerId(this.serverId);
        Router rpcRouter = new Router(this.serverGroup, selfServerId);
        ActorSystem actorSystem = ActorSystem.create(this.actionSystemName);
        ActorRef electionActor = actorSystem.actorOf(
                Props.create(ElectionActor.class, this.serverGroup, selfServerId, this.serverState, rpcRouter),
                "election"
        );

        EmbeddedChannel rpcChannel = new EmbeddedChannel();
        rpcChannel.addListener(new ElectionActorRpcChannelListener(actorSystem));
        Server server = new Server(selfServerId, actorSystem, rpcChannel);
        serverGroup.addServer(server);
        return server;
    }

}
