package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.Endpoint;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class NodeConfig {

    private final NodeId id;
    private final Endpoint endpoint;

    public NodeConfig(String id, String host, int port) {
        this.id = new NodeId(id);
        this.endpoint = new Endpoint(host, port);
    }

    public NodeId getId() {
        return this.id;
    }

    public String getHost() {
        return this.endpoint.getHost();
    }

    public int getPort() {
        return this.endpoint.getPort();
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeConfig)) return false;
        NodeConfig that = (NodeConfig) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "NodeConfig{id=" + id + ", endpoint=" + endpoint + '}';
    }

    public static Set<NodeConfig> merge(Set<NodeConfig> oldNodeConfigs, Set<NodeConfig> newNodeConfigs) {
        Set<NodeConfig> nodeConfigs = new HashSet<>(oldNodeConfigs);
        nodeConfigs.addAll(newNodeConfigs);
        return nodeConfigs;
    }

    public static boolean isSame(Set<NodeConfig> nodeConfigs1, Set<NodeConfig> nodeConfigs2) {
        if (nodeConfigs1.size() != nodeConfigs2.size()) return false;

        for (NodeConfig config : nodeConfigs1) {
            if (!nodeConfigs2.contains(config)) {
                return false;
            }
        }
        return true;
    }

}
