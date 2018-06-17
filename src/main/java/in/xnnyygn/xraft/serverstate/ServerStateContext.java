package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.schedule.LogReplicationTask;
import in.xnnyygn.xraft.messages.Message;
import in.xnnyygn.xraft.server.ServerId;

// TODO rename to a better one
public interface ServerStateContext extends ElectionTimeoutScheduler {

    ServerId getSelfServerId();

    int getServerCount();

    void setServerState(AbstractServerState serverState);

    LogReplicationTask scheduleLogReplicationTask();

    void sendRpcOrResultMessage(Message message);

}
