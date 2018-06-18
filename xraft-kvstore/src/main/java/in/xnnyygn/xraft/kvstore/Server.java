package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.core.service.NodeStateException;
import in.xnnyygn.xraft.kvstore.command.GetCommand;
import in.xnnyygn.xraft.kvstore.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final Node node;
    private final Service service;
    private final int port;

    private Protocol protocol = new Protocol();
    private Thread thread;

    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public Server(Node node, Service service, int port) {
        this.node = node;
        this.service = service;
        this.port = port;
    }

    public void start() throws Exception {
        this.node.start();

        this.running = true;
        this.serverSocket = new ServerSocket(this.port);
        this.thread = new Thread(() -> {
            logger.info("Server {}, listener on port {}", node.getId(), port);
            while (running) {
                try (Socket socket = serverSocket.accept()) {
                    handleRequest(socket);
                } catch (IOException e) {
                    logger.warn("failed to handle request", e);
                }
            }
        }, "kvstore-server-" + this.node.getId());
        this.thread.start();
    }

    private void handleRequest(Socket socket) throws IOException {
        logger.debug("accept connection from {}", socket.getRemoteSocketAddress());
        while (true) {
            Object request = this.protocol.fromWire(socket.getInputStream());
            if (request == Protocol.PAYLOAD_QUIT) break;
            logger.debug("receive request {}", request);

            // check leadership
            NodeStateSnapshot state = this.node.getNodeState();
            if (state.getRole() == NodeRole.FOLLOWER) {
                this.protocol.toWire(new NodeStateException(NodeRole.FOLLOWER, state.getLeaderId()), socket.getOutputStream());
                continue;
            }
            if (state.getRole() == NodeRole.CANDIDATE) {
                this.protocol.toWire(new NodeStateException(NodeRole.CANDIDATE, null), socket.getOutputStream());
                continue;
            }

            if (request instanceof GetCommand) {
                Object result = this.service.get(((GetCommand) request).getKey());
                this.protocol.toWire(result, socket.getOutputStream());
            } else if (request instanceof SetCommand) {
                SetCommand command = (SetCommand) request;
                this.service.set(command.getKey(), command.getValue());
                this.protocol.toWire(Protocol.PAYLOAD_VOID, socket.getOutputStream());
            }
        }
        logger.debug("close socket");
    }

    public void stop() throws Exception {
        this.node.stop();
        this.running = false;
        this.serverSocket.close();
        this.thread.join();
    }

}
