package in.xnnyygn.xraft.core.rpc.socket;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.*;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.rpc.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SocketConnector extends AbstractConnector implements SocketChannelContext {

    private static final Logger logger = LoggerFactory.getLogger(SocketConnector.class);
    private final EventBus eventBus;

    private final DirectionalChannelRegister channelRegister;
    private final SocketChannelExecutorServicePool channelExecutorServicePool;

    private int port;
    private ServerSocket serverSocket;
    private Thread acceptorThread;

    private ExecutorService executorService;


    public SocketConnector(NodeGroup nodeGroup, NodeId selfNodeId, EventBus eventBus, int port) {
        super(nodeGroup, selfNodeId);
        this.eventBus = eventBus;
        this.port = port;

        this.channelRegister = new DirectionalChannelRegister();
        this.channelExecutorServicePool = new SocketChannelExecutorServicePool();

        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "socket-connector"));
    }

    @Override
    public void initialize() {
        try {
            this.serverSocket = new ServerSocket();
            logger.debug("start acceptor at port {}", this.port);
            this.serverSocket.bind(new InetSocketAddress(this.port));
        } catch (IOException e) {
            throw new ChannelException("failed to bind on port " + this.port, e);
        }

        this.acceptorThread = new Thread(this::accept, "socket-connector-acceptor-" + this.selfNodeId);
        this.acceptorThread.start();
    }

    @Override
    public void resetChannels() {
        this.executorService.submit(this.channelRegister::closeInboundChannels);
    }

    private void accept() {
        while (!this.serverSocket.isClosed()) {
            Socket socket = null;
            try {
                socket = this.serverSocket.accept();
                SocketChannel channel = new SocketChannel(socket, SocketChannel.Direction.INBOUND, this.eventBus,
                        this.channelExecutorServicePool.borrowExecutorService(), this);
                channel.open();
            } catch (Exception e) {
                if (!this.serverSocket.isClosed()) break;

                logger.debug("failed to open, cause " + e.getMessage());
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {
        this.executorService.submit(() -> super.sendRequestVote(rpc));
    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {
        this.executorService.submit(() -> super.replyRequestVote(result, rpcMessage));
    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {
        this.executorService.submit(() -> super.sendAppendEntries(rpc, destinationNodeId));
    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {
        this.executorService.submit(() -> super.replyAppendEntries(result, rpcMessage));
    }

    @Override
    protected Channel getChannel(AbstractNode node) {
        // in thread node-state-machine
        NodeId nodeId = node.getId();
        SocketChannel channel = (SocketChannel) this.channelRegister.find(nodeId);
        if (channel != null) return channel;

        Endpoint endpoint = node.getEndpoint();
        if (!(endpoint instanceof SocketEndpoint)) {
            throw new IllegalArgumentException("expect socket endpoint");
        }

        Socket socket = null;
        try {
            logger.debug("connect to node {}, endpoint {}", nodeId, endpoint);
            socket = ((SocketEndpoint) endpoint).connect();
            channel = new SocketChannel(socket, SocketChannel.Direction.OUTBOUND, eventBus,
                    this.channelExecutorServicePool.borrowExecutorService(), this);
            channel.setRemoteId(nodeId);
            channel.open(this.selfNodeId);
        } catch (Exception e) {
            logger.debug("failed to open, cause " + e.getMessage());
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
            throw new ChannelException("failed to connect, cause " + e.getMessage(), e);
        }

        this.channelRegister.register(channel);
        return channel;
    }

    @Override
    protected <T> Channel getChannel(AbstractRpcMessage<T> rpcMessage) {
        // in thread node-state-machine
        this.channelRegister.register((SocketChannel) rpcMessage.getChannel());
        return rpcMessage.getChannel();
    }

    @Override
    public void closeChannel(SocketChannel channel) {
        this.executorService.submit(() -> {
            this.channelRegister.remove(channel);
            channel.close();
            this.channelExecutorServicePool.returnExecutorService(channel.getExecutorService());
        });
    }

    @Override
    public void release() {
        logger.debug("release connector");
        try {
            this.serverSocket.close();
        } catch (IOException ignored) {
        }

        try {
            this.acceptorThread.join();
        } catch (InterruptedException ignored) {
        }

        Future<?> future = this.executorService.submit(() -> {
            this.channelRegister.release();
            this.channelExecutorServicePool.release();
        });
        try {
            future.get();
        } catch (Exception ignored) {
        }

        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

}
