package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.service.Channel;
import in.xnnyygn.xraft.core.service.ChannelException;

import java.io.IOException;
import java.net.InetSocketAddress;
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
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(this.host, this.port));
            protocol.toWire(payload, socket.getOutputStream());
            return protocol.fromWire(socket.getInputStream());
        } catch (IOException e) {
            throw new ChannelException("failed to send", e);
        }
    }

}
