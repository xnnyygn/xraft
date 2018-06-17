package in.xnnyygn.xraft.server;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RaftNodeGroup implements Iterable<AbstractRaftNode> {

    private Map<ServerId, AbstractRaftNode> nodeMap;

    public RaftNodeGroup() {
        this.nodeMap = new HashMap<>();
    }

    public void addNode(AbstractRaftNode node) {
        this.nodeMap.put(node.getId(), node);
    }

    public int getNodeCount() {
        return this.nodeMap.size();
    }

    @Deprecated
    public ServerId getSelfId() {
        throw new UnsupportedOperationException();
    }

    public void startAll() {
        for (AbstractRaftNode node : nodeMap.values()) {
            if (node instanceof Server) {
                ((Server) node).start();
            }
        }
    }

    public void stopAll() {
        for (AbstractRaftNode node : nodeMap.values()) {
            if (node instanceof Server) {
                ((Server) node).stop();
            }
        }
    }

    @Override
    public Iterator<AbstractRaftNode> iterator() {
        return this.nodeMap.values().iterator();
    }

    public AbstractRaftNode findNode(ServerId nodeId) {
        return this.nodeMap.get(nodeId);
    }

}
