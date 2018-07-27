package in.xnnyygn.xraft.core.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

public class NodeGroup implements Iterable<NodeConfig> {

    private static final Logger logger = LoggerFactory.getLogger(NodeGroup.class);
    private Map<NodeId, NodeConfig> nodeConfigMap;

    public NodeGroup(Set<NodeConfig> nodeConfigs) {
        this.nodeConfigMap = buildNodeConfigMap(nodeConfigs);
    }

    private Map<NodeId, NodeConfig> buildNodeConfigMap(Set<NodeConfig> nodeConfigs) {
        Map<NodeId, NodeConfig> map = new HashMap<>();
        for (NodeConfig config : nodeConfigs) {
            map.put(config.getId(), config);
        }
        return map;
    }

    public int getCount() {
        return this.nodeConfigMap.size();
    }

    public NodeConfig find(NodeId id) {
        NodeConfig config = this.nodeConfigMap.get(id);
        if (config == null) {
            throw new IllegalStateException("no config for node " + id);
        }
        return config;
    }

    public Set<NodeId> getIds() {
        return Collections.unmodifiableSet(this.nodeConfigMap.keySet());
    }

    @Override
    @Nonnull
    public Iterator<NodeConfig> iterator() {
        return this.nodeConfigMap.values().iterator();
    }

    public Set<NodeConfig> toNodeConfigs() {
        return new HashSet<>(this.nodeConfigMap.values());
    }

    public void addNode(NodeConfig config) {
        logger.info("add node {} to group", config);
        this.nodeConfigMap.put(config.getId(), config);
    }

    public void removeNode(NodeId nodeId) {
        logger.info("remove node {}", nodeId);
        this.nodeConfigMap.remove(nodeId);
    }

}
