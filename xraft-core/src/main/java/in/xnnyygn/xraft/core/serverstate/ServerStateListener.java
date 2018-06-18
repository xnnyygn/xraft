package in.xnnyygn.xraft.core.serverstate;

public interface ServerStateListener {

    void serverStateChanged(ServerStateSnapshot snapshot);

}
