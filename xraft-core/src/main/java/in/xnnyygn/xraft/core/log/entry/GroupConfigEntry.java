package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.util.Set;

public abstract class GroupConfigEntry extends AbstractEntry {

    private final Set<NodeEndpoint> nodeEndpoints;

    protected GroupConfigEntry(int kind, int index, int term, Set<NodeEndpoint> nodeEndpoints) {
        super(kind, index, term);
        this.nodeEndpoints = nodeEndpoints;
    }

    public Set<NodeEndpoint> getNodeEndpoints() {
        return nodeEndpoints;
    }

    public abstract Set<NodeEndpoint> getResultNodeEndpoints();

}
