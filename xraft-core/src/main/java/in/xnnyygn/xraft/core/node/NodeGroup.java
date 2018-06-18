package in.xnnyygn.xraft.core.node;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NodeGroup implements Iterable<AbstractNode> {

    private Map<NodeId, AbstractNode> nodeMap;

    public NodeGroup() {
        this.nodeMap = new HashMap<>();
    }

    public void add(AbstractNode server) {
        this.nodeMap.put(server.getId(), server);
    }

    public int getCount() {
        return this.nodeMap.size();
    }

    public void startAll() {
        for (AbstractNode node : nodeMap.values()) {
            if (node instanceof Node) {
                ((Node) node).start();
            }
        }
    }

    public void stopAll() throws Exception {
        for (AbstractNode server : nodeMap.values()) {
            if (server instanceof Node) {
                ((Node) server).stop();
            }
        }
    }

    @Override
    public Iterator<AbstractNode> iterator() {
        return this.nodeMap.values().iterator();
    }

    public AbstractNode find(NodeId nodeId) {
        return this.nodeMap.get(nodeId);
    }

}
