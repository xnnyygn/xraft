package in.xnnyygn.xraft.core.nodestate;

public interface NodeStateListener {

    void nodeStateChanged(NodeStateSnapshot snapshot);

}
