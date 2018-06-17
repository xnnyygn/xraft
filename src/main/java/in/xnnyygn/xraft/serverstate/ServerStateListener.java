package in.xnnyygn.xraft.serverstate;

public interface ServerStateListener {

    void serverStateChanged(ServerStateSnapshot snapshot);

}
