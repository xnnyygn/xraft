package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final Node node;
    private final Service service;
    private final int port;

    private Thread thread;
    private TServerTransport transport;
    private TServer server;

    public Server(Node node, Service service, int port) {
        this.node = node;
        this.service = service;
        this.port = port;
    }

    public void start() throws Exception {
        this.node.start();

        ServiceAdapter handler = new ServiceAdapter(this.node, this.service);
        KVStore.Processor<KVStore.Iface> processor = new KVStore.Processor<>(handler);
        transport = new TServerSocket(this.port);
        server = new TSimpleServer(new TServer.Args(transport).processor(processor));
        this.thread = new Thread(() -> {
            logger.info("Server {}, listen on port {}", node.getId(), port);
            server.serve();
        }, "kvstore-server-" + this.node.getId());
        this.thread.start();
    }

    public void stop() throws Exception {
        this.node.stop();
        this.transport.close();
        this.server.stop();
        this.thread.join();
    }

}
