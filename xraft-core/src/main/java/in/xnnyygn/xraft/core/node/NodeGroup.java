package in.xnnyygn.xraft.core.node;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class NodeGroup implements Iterable<AbstractNode> {

    private Map<NodeId, AbstractNode> nodeMap;

    public NodeGroup() {
        this.nodeMap = new HashMap<>();
    }

    public void add(AbstractNode node) {
        this.nodeMap.put(node.getId(), node);
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
        for (AbstractNode node : nodeMap.values()) {
            if (node instanceof Node) {
                ((Node) node).stop();
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

    public Set<NodeId> getNodeIds() {
        return this.nodeMap.keySet();
    }

}
