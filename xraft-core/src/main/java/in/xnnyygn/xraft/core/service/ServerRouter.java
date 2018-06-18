package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ServerRouter {

    private static Logger logger = LoggerFactory.getLogger(ServerRouter.class);
    private final Map<ServerId, Channel> available = new HashMap<>();
    private ServerId leaderId;

    public Object send(Object payload) {
        try {
            return doSend(getCurrentLeaderId(), payload);
        } catch (RedirectException e) {
            logger.info("not a leader server, redirect to server {}", e.getLeaderId());
            this.leaderId = e.getLeaderId();
            return doSend(e.getLeaderId(), payload);
        }
    }

    private ServerId getCurrentLeaderId() {
        if (this.leaderId != null) return this.leaderId;

        if (this.available.isEmpty()) {
            throw new IllegalStateException("no available server");
        }
        return this.available.keySet().iterator().next();
    }

    private Object doSend(ServerId id, Object payload) {
        Channel channel = this.available.get(id);
        if (channel == null) {
            throw new IllegalStateException("no such channel to server " + id);
        }
        logger.info("send request to server {}", id);
        return channel.send(payload);
    }

    public void add(ServerId id, Channel channel) {
        this.available.put(id, channel);
    }

}
