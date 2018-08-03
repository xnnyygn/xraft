package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.log.Protos;
import in.xnnyygn.xraft.core.node.NodeConfig;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class AddNodeEntry extends GroupConfigEntry {

    private final NodeConfig newNodeConfig;

    public AddNodeEntry(int index, int term, Set<NodeConfig> nodeConfigs, NodeConfig newNodeConfig) {
        super(KIND_ADD_NODE, index, term, nodeConfigs);
        this.newNodeConfig = newNodeConfig;
    }

    public NodeConfig getNewNodeConfig() {
        return newNodeConfig;
    }

    public Set<NodeConfig> getResultNodeConfigs() {
        Set<NodeConfig> configs = new HashSet<>(getNodeConfigs());
        configs.add(newNodeConfig);
        return configs;
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.AddNodeCommand.newBuilder()
                .addAllNodeConfigs(getNodeConfigs().stream().map(c ->
                        Protos.NodeConfig.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getHost())
                                .setPort(c.getPort())
                                .build()
                ).collect(Collectors.toList()))
                .setNewNodeConfig(Protos.NodeConfig.newBuilder()
                        .setId(newNodeConfig.getId().getValue())
                        .setHost(newNodeConfig.getHost())
                        .setPort(newNodeConfig.getPort())
                        .build()
                ).build().toByteArray();
    }

    @Override
    public String toString() {
        return "AddNodeEntry{" +
                "index=" + index +
                ", term=" + term +
                ", nodeConfigs=" + getNodeConfigs() +
                ", newNodeConfig=" + newNodeConfig +
                '}';
    }

}
