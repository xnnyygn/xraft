package in.xnnyygn.xraft.core.rpc.message;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

public class RemoveServerRpc {

    private final NodeEndpoint oldServer;

    public RemoveServerRpc(NodeEndpoint oldServer) {
        this.oldServer = oldServer;
    }

    public NodeEndpoint getOldServer() {
        return oldServer;
    }

    @Override
    public String toString() {
        return "RemoveServerRpc{" +
                "oldServer=" + oldServer +
                '}';
    }

}
