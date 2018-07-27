package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.log.Protos;
import in.xnnyygn.xraft.core.node.NodeConfig;

import java.util.Set;
import java.util.stream.Collectors;

public class GroupConfigEntry extends AbstractEntry {

    private final Set<NodeConfig> nodeConfigs;

    public GroupConfigEntry(int index, int term, Protos.GroupConfigCommand command) {
        this(index, term, command.getNodeConfigsList().stream().map(c ->
                new NodeConfig(c.getId(), c.getHost(), c.getPort())
        ).collect(Collectors.toSet()));
    }

    public GroupConfigEntry(int index, int term, Set<NodeConfig> nodeConfigs) {
        super(KIND_GROUP_CONFIG, index, term);
        this.nodeConfigs = nodeConfigs;
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.GroupConfigCommand.newBuilder().addAllNodeConfigs(
                this.nodeConfigs.stream().map(c ->
                        Protos.GroupConfigCommand.NodeConfig.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getHost())
                                .setPort(c.getPort())
                                .build()
                ).collect(Collectors.toList())
        ).build().toByteArray();
    }

    public Set<NodeConfig> getNodeConfigs() {
        return nodeConfigs;
    }

}
