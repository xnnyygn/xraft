package in.xnnyygn.xraft.core.serverstate;

import in.xnnyygn.xraft.core.rpc.Router;
import in.xnnyygn.xraft.core.schedule.ElectionTimeoutScheduler;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.server.ServerId;

// TODO rename to a better one
public interface ServerStateContext extends ElectionTimeoutScheduler {

    ServerId getSelfServerId();

    int getServerCount();

    void setServerState(AbstractServerState serverState);

    LogReplicationTask scheduleLogReplicationTask();

    Router getRpcRouter();

}
