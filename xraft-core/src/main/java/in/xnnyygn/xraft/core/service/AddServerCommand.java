package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

// TODO service.proto?
public class AddServerCommand {

    private final String nodeId;
    private final String host;
    private final int port;

    public AddServerCommand(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NodeEndpoint toNodeConfig() {
        return new NodeEndpoint(nodeId, host, port);
    }

}
