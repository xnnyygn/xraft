package in.xnnyygn.xraft.core.rpc.socket;

import in.xnnyygn.xraft.core.rpc.Endpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketEndpoint implements Endpoint {

    private final String host;
    private final int port;

    public SocketEndpoint(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(this.host, this.port));
        return socket;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "SocketEndpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

}
