package in.xnnyygn.xraft.core.log.entry;

import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class EntryFactory {

    public Entry create(int kind, int index, int term, byte[] commandBytes) {
        try {
            switch (kind) {
                case Entry.KIND_NO_OP:
                    return new NoOpEntry(index, term);
                case Entry.KIND_GENERAL:
                    return new GeneralEntry(index, term, commandBytes);
                case Entry.KIND_ADD_NODE:
                    Protos.AddNodeCommand addNodeCommand = Protos.AddNodeCommand.parseFrom(commandBytes);
                    return new AddNodeEntry(index, term, asNodeEndpoints(addNodeCommand.getNodeEndpointsList()), asNodeEndpoint(addNodeCommand.getNewNodeEndpoint()));
                case Entry.KIND_REMOVE_NODE:
                    Protos.RemoveNodeCommand removeNodeCommand = Protos.RemoveNodeCommand.parseFrom(commandBytes);
                    return new RemoveNodeEntry(index, term, asNodeEndpoints(removeNodeCommand.getNodeEndpointsList()), new NodeId(removeNodeCommand.getNodeToRemove()));
                default:
                    throw new IllegalArgumentException("unexpected entry kind " + kind);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to parse command", e);
        }
    }

    private Set<NodeEndpoint> asNodeEndpoints(Collection<Protos.NodeEndpoint> protoNodeEndpoints) {
        return protoNodeEndpoints.stream().map(this::asNodeEndpoint).collect(Collectors.toSet());
    }

    private NodeEndpoint asNodeEndpoint(Protos.NodeEndpoint protoNodeEndpoint) {
        return new NodeEndpoint(protoNodeEndpoint.getId(), protoNodeEndpoint.getHost(), protoNodeEndpoint.getPort());
    }

}
