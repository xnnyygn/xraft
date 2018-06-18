package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.server.ServerId;
import in.xnnyygn.xraft.core.serverstate.ServerRole;

public class ServerStateException extends RuntimeException {

    private final ServerRole role;
    private final ServerId leaderId;

    public ServerStateException(ServerRole role, ServerId leaderId) {
        this.role = role;
        this.leaderId = leaderId;
    }

    @Override
    public String getMessage() {
        return "unexpected server state, role " + this.role + ", leader id " + this.leaderId;
    }

    public ServerRole getRole() {
        return role;
    }

    public ServerId getLeaderId() {
        return leaderId;
    }

    public boolean isLeaderIdPresent() {
        return this.leaderId != null;
    }

}
