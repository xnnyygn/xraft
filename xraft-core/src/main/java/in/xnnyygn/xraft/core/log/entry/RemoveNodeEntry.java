package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Set;
import java.util.stream.Collectors;

public class RemoveNodeEntry extends GroupConfigEntry {

    private final NodeId nodeToRemove;

    public RemoveNodeEntry(int index, int term, Set<NodeEndpoint> nodeEndpoints, NodeId nodeToRemove) {
        super(KIND_REMOVE_NODE, index, term, nodeEndpoints);
        this.nodeToRemove = nodeToRemove;
    }

    public Set<NodeEndpoint> getResultNodeEndpoints() {
        return getNodeEndpoints().stream()
                .filter(c -> !c.getId().equals(nodeToRemove))
                .collect(Collectors.toSet());
    }

    public NodeId getNodeToRemove() {
        return nodeToRemove;
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.RemoveNodeCommand.newBuilder()
                .addAllNodeEndpoints(getNodeEndpoints().stream().map(c ->
                        Protos.NodeEndpoint.newBuilder()
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
                ", nodeEndpoints=" + getNodeEndpoints() +
                ", nodeToRemove=" + nodeToRemove +
                '}';
    }

}
