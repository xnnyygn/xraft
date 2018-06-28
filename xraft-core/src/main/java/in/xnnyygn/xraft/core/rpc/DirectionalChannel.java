package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

public interface DirectionalChannel extends Channel {

    enum Direction {
        INBOUND, OUTBOUND;
    }

    NodeId getRemoteId();

    Direction getDirection();

}
