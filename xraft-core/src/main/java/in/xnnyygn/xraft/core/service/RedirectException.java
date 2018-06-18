package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.server.ServerId;

public class RedirectException extends RuntimeException {

    private final ServerId leaderId;

    public RedirectException(ServerId leaderId) {
        this.leaderId = leaderId;
    }

    public ServerId getLeaderId() {
        return leaderId;
    }

}
