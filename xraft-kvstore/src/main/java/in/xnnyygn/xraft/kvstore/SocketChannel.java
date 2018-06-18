package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.service.Channel;
import in.xnnyygn.xraft.core.service.ChannelException;
import in.xnnyygn.xraft.core.service.NodeStateException;
import in.xnnyygn.xraft.core.service.RedirectException;

import java.io.IOException;
import java.net.Socket;

public class SocketChannel implements Channel {

    private final String host;
    private final int port;
    private Protocol protocol = new Protocol();

    public SocketChannel(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public Object send(Object payload) {
        try (Socket socket = new Socket(this.host, this.port)) {
            protocol.toWire(payload, socket.getOutputStream());
            Object result = protocol.fromWire(socket.getInputStream());
            protocol.toWire(Protocol.PAYLOAD_QUIT, socket.getOutputStream());
            if (result instanceof NodeStateException) {
                NodeStateException exception = (NodeStateException) result;
                if (exception.getRole() == NodeRole.FOLLOWER && exception.isLeaderIdPresent()) {
                    throw new RedirectException(exception.getLeaderId());
                }
                throw exception;
            }
            return result;
        } catch (IOException e) {
            throw new ChannelException("failed to send", e);
        }
    }

}
