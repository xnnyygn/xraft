package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.node.NodeConfig;

import java.util.Set;

public abstract class GroupConfigEntry extends AbstractEntry {

    private final Set<NodeConfig> nodeConfigs;

    protected GroupConfigEntry(int kind, int index, int term, Set<NodeConfig> nodeConfigs) {
        super(kind, index, term);
        this.nodeConfigs = nodeConfigs;
    }

    public Set<NodeConfig> getNodeConfigs() {
        return nodeConfigs;
    }

    public abstract Set<NodeConfig> getResultNodeConfigs();

}
