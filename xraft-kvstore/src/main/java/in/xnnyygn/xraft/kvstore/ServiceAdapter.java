package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import org.apache.thrift.TException;

public class ServiceAdapter implements KVStore.Iface {

    private final Node node;
    private final Service service;

    public ServiceAdapter(Node node, Service service) {
        this.node = node;
        this.service = service;
    }

    @Override
    public void Set(String key, String value) throws TException {
        checkLeadership();
        this.service.set(key, value);
    }

    @Override
    public GetResult Get(String key) throws TException {
        checkLeadership();
        String value = this.service.get(key);

        GetResult result = new GetResult();
        result.setFound(value != null);
        result.setValue(value);
        return result;
    }

    private void checkLeadership() throws Redirect {
        NodeStateSnapshot state = this.node.getNodeState();
        if (state.getRole() == NodeRole.FOLLOWER) {
            NodeId leaderId = state.getLeaderId();
            throw new Redirect(leaderId != null ? leaderId.getValue() : null);
        }
        if (state.getRole() == NodeRole.CANDIDATE) {
            throw new Redirect((String) null);
        }
    }

}