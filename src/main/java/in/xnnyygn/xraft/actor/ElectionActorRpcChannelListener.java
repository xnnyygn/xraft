package in.xnnyygn.xraft.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import in.xnnyygn.xraft.messages.AppendEntriesResultMessage;
import in.xnnyygn.xraft.messages.AppendEntriesRpcMessage;
import in.xnnyygn.xraft.messages.RequestVoteResultMessage;
import in.xnnyygn.xraft.messages.RequestVoteRpcMessage;
import in.xnnyygn.xraft.rpc.*;
import in.xnnyygn.xraft.server.ServerId;

public class ElectionActorRpcChannelListener implements ChannelListener {

    private final ActorSystem actorSystem;

    public ElectionActorRpcChannelListener(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    public ActorSelection getElectionActor() {
        return this.actorSystem.actorSelection("/user/election");
    }

    @Override
    public void receive(Object payload, ServerId senderId) {
        if (payload instanceof RequestVoteRpc) {
            getElectionActor().tell(new RequestVoteRpcMessage((RequestVoteRpc) payload), ActorRef.noSender());
        } else if (payload instanceof RequestVoteResult) {
            RequestVoteResultMessage msg = new RequestVoteResultMessage((RequestVoteResult) payload);
            msg.setSenderServerId(senderId);
            getElectionActor().tell(msg, ActorRef.noSender());
        } else if (payload instanceof AppendEntriesRpc) {
            getElectionActor().tell(new AppendEntriesRpcMessage((AppendEntriesRpc) payload), ActorRef.noSender());
        } else if (payload instanceof AppendEntriesResult) {
            AppendEntriesResultMessage msg = new AppendEntriesResultMessage((AppendEntriesResult) payload);
            msg.setSenderServerId(senderId);
            getElectionActor().tell(msg, ActorRef.noSender());
        }
    }

}
