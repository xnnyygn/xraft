package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class AddNodeEntry extends GroupConfigEntry {

    private final NodeEndpoint newNodeEndpoint;

    public AddNodeEntry(int index, int term, Set<NodeEndpoint> nodeEndpoints, NodeEndpoint newNodeEndpoint) {
        super(KIND_ADD_NODE, index, term, nodeEndpoints);
        this.newNodeEndpoint = newNodeEndpoint;
    }

    public NodeEndpoint getNewNodeEndpoint() {
        return newNodeEndpoint;
    }

    public Set<NodeEndpoint> getResultNodeEndpoints() {
        Set<NodeEndpoint> configs = new HashSet<>(getNodeEndpoints());
        configs.add(newNodeEndpoint);
        return configs;
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.AddNodeCommand.newBuilder()
                .addAllNodeEndpoints(getNodeEndpoints().stream().map(c ->
                        Protos.NodeEndpoint.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getHost())
                                .setPort(c.getPort())
                                .build()
                ).collect(Collectors.toList()))
                .setNewNodeEndpoint(Protos.NodeEndpoint.newBuilder()
                        .setId(newNodeEndpoint.getId().getValue())
                        .setHost(newNodeEndpoint.getHost())
                        .setPort(newNodeEndpoint.getPort())
                        .build()
                ).build().toByteArray();
    }

    @Override
    public String toString() {
        return "AddNodeEntry{" +
                "index=" + index +
                ", term=" + term +
                ", nodeEndpoints=" + getNodeEndpoints() +
                ", newNodeEndpoint=" + newNodeEndpoint +
                '}';
    }

}
