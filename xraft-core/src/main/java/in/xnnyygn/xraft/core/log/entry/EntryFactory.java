package in.xnnyygn.xraft.core.log.entry;

import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.core.log.Protos;
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
                    return new AddNodeEntry(index, term, asNodeConfigs(addNodeCommand.getNodeConfigsList()), asNodeConfig(addNodeCommand.getNewNodeConfig()));
                case Entry.KIND_REMOVE_NODE:
                    Protos.RemoveNodeCommand removeNodeCommand = Protos.RemoveNodeCommand.parseFrom(commandBytes);
                    return new RemoveNodeEntry(index, term, asNodeConfigs(removeNodeCommand.getNodeConfigsList()), new NodeId(removeNodeCommand.getNodeToRemove()));
                default:
                    throw new IllegalArgumentException("unexpected entry kind " + kind);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to parse command", e);
        }
    }

    private Set<NodeEndpoint> asNodeConfigs(Collection<Protos.NodeConfig> protoNodeConfigs) {
        return protoNodeConfigs.stream().map(this::asNodeConfig).collect(Collectors.toSet());
    }

    private NodeEndpoint asNodeConfig(Protos.NodeConfig protoNodeConfig) {
        return new NodeEndpoint(protoNodeConfig.getId(), protoNodeConfig.getHost(), protoNodeConfig.getPort());
    }

}
