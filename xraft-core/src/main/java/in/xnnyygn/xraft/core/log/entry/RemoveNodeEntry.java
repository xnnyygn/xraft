package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.log.Protos;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Set;
import java.util.stream.Collectors;

public class RemoveNodeEntry extends GroupConfigEntry {

    private final NodeId nodeToRemove;

    public RemoveNodeEntry(int index, int term, Set<NodeConfig> nodeConfigs, NodeId nodeToRemove) {
        super(KIND_REMOVE_NODE, index, term, nodeConfigs);
        this.nodeToRemove = nodeToRemove;
    }

    public Set<NodeConfig> getResultNodeConfigs() {
        return getNodeConfigs().stream()
                .filter(c -> !c.getId().equals(nodeToRemove))
                .collect(Collectors.toSet());
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.RemoveNodeCommand.newBuilder()
                .addAllNodeConfigs(getNodeConfigs().stream().map(c ->
                        Protos.NodeConfig.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getHost())
                                .setPort(c.getPort())
                                .build()
                ).collect(Collectors.toList()))
                .setNodeToRemove(nodeToRemove.getValue())
                .build().toByteArray();
    }

    @Override
    public String toString() {
        return "RemoveNodeEntry{" +
                "index=" + index +
                ", term=" + term +
                ", nodeConfigs=" + getNodeConfigs() +
                ", nodeToRemove=" + nodeToRemove +
                '}';
    }

}
