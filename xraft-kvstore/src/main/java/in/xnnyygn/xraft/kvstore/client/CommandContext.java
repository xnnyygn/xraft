package in.xnnyygn.xraft.kvstore.client;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.service.ServerRouter;

import java.util.Map;

class CommandContext {

    private final Map<NodeId, Endpoint> serverMap;
    private Client client;
    private boolean running = false;

    public CommandContext(Map<NodeId, Endpoint> serverMap) {
        this.serverMap = serverMap;
        this.client = new Client(buildServerRouter(serverMap));
    }

    private ServerRouter buildServerRouter(Map<NodeId, Endpoint> serverMap) {
        ServerRouter router = new ServerRouter();
        for (NodeId nodeId : serverMap.keySet()) {
            Endpoint endpoint = serverMap.get(nodeId);
            router.add(nodeId, new SocketChannel(endpoint.getHost(), endpoint.getPort()));
        }
        return router;
    }

    Client getClient() {
        return client;
    }

    NodeId getClientLeader() {
        return client.getServerRouter().getLeaderId();
    }

    void setClientLeader(NodeId nodeId) {
        client.getServerRouter().setLeaderId(nodeId);
    }

    void clientAddServer(String nodeId, String host, int portService) {
        serverMap.put(new NodeId(nodeId), new Endpoint(host, portService));
        client = new Client(buildServerRouter(serverMap));
    }

    boolean clientRemoveServer(String nodeId) {
        Endpoint endpoint = serverMap.remove(new NodeId(nodeId));
        if (endpoint != null) {
            client = new Client(buildServerRouter(serverMap));
            return true;
        }
        return false;
    }

    void setRunning(boolean running) {
        this.running = running;
    }

    boolean isRunning() {
        return running;
    }

    void printSeverList() {
        for (NodeId nodeId : serverMap.keySet()) {
            Endpoint endpoint = serverMap.get(nodeId);
            System.out.println(nodeId + "," + endpoint.getHost() + "," + endpoint.getPort());
        }
    }

}
