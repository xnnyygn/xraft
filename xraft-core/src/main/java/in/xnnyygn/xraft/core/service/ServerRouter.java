package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ServerRouter {

    private static Logger logger = LoggerFactory.getLogger(ServerRouter.class);
    private final Map<NodeId, Channel> availableServers = new HashMap<>();
    private NodeId leaderId;

    public Object send(Object payload) {
        for (NodeId nodeId : getCandidateNodeIds()) {
            try {
                Object result = doSend(nodeId, payload);
                this.leaderId = nodeId;
                return result;
            } catch (RedirectException e) {
                logger.debug("not a leader server");
                NodeId newLeaderId = e.getLeaderId();
                if (newLeaderId != null) {
                    this.leaderId = newLeaderId;
                    return doSend(newLeaderId, payload);
                }
            } catch (Exception e) {
                logger.debug("failed to process with server " + nodeId, e);
            }
        }
        throw new NoAvailableServerException("no available server");
    }

    private Collection<NodeId> getCandidateNodeIds() {
        if (availableServers.isEmpty()) {
            throw new NoAvailableServerException("no available server");
        }

        if (leaderId != null) {
            List<NodeId> nodeIds = new ArrayList<>();
            nodeIds.add(leaderId);
            for (NodeId nodeId : availableServers.keySet()) {
                if (!nodeId.equals(leaderId)) {
                    nodeIds.add(nodeId);
                }
            }
            return nodeIds;
        }

        return availableServers.keySet();
    }

    private Object doSend(NodeId id, Object payload) {
        Channel channel = this.availableServers.get(id);
        if (channel == null) {
            throw new IllegalStateException("no such channel to server " + id);
        }
        logger.debug("send request to server {}", id);
        return channel.send(payload);
    }

    public void add(NodeId id, Channel channel) {
        this.availableServers.put(id, channel);
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
        if (!availableServers.containsKey(leaderId)) {
            throw new IllegalStateException("no such server [" + leaderId + "] in list");
        }
        this.leaderId = leaderId;
    }

}
