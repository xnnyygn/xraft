package in.xnnyygn.xraft.server;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ServerGroup implements Iterable<AbstractServer> {

    private Map<ServerId, AbstractServer> nodeMap;

    public ServerGroup() {
        this.nodeMap = new HashMap<>();
    }

    public void addNode(AbstractServer node) {
        this.nodeMap.put(node.getId(), node);
    }

    public int getServerCount() {
        return this.nodeMap.size();
    }

    @Deprecated
    public ServerId getSelfId() {
        throw new UnsupportedOperationException();
    }

    public void startAll() {
        for (AbstractServer node : nodeMap.values()) {
            if (node instanceof Server) {
                ((Server) node).start();
            }
        }
    }

    public void stopAll() {
        for (AbstractServer node : nodeMap.values()) {
            if (node instanceof Server) {
                ((Server) node).stop();
            }
        }
    }

    @Override
    public Iterator<AbstractServer> iterator() {
        return this.nodeMap.values().iterator();
    }

    public AbstractServer findNode(ServerId nodeId) {
        return this.nodeMap.get(nodeId);
    }

}
