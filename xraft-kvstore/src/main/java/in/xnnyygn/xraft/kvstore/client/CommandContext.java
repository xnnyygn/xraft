package in.xnnyygn.xraft.kvstore.client;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Address;
import in.xnnyygn.xraft.core.service.ServerRouter;

import java.util.Map;

class CommandContext {

    private final Map<NodeId, Address> serverMap;
    private Client client;
    private boolean running = false;

    public CommandContext(Map<NodeId, Address> serverMap) {
        this.serverMap = serverMap;
        this.client = new Client(buildServerRouter(serverMap));
    }

    private ServerRouter buildServerRouter(Map<NodeId, Address> serverMap) {
        ServerRouter router = new ServerRouter();
        for (NodeId nodeId : serverMap.keySet()) {
            Address address = serverMap.get(nodeId);
            router.add(nodeId, new SocketChannel(address.getHost(), address.getPort()));
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
        serverMap.put(new NodeId(nodeId), new Address(host, portService));
        client = new Client(buildServerRouter(serverMap));
    }

    boolean clientRemoveServer(String nodeId) {
        Address address = serverMap.remove(new NodeId(nodeId));
        if (address != null) {
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
            Address address = serverMap.get(nodeId);
            System.out.println(nodeId + "," + address.getHost() + "," + address.getPort());
        }
    }

}
