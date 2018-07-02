package in.xnnyygn.xraft.core.rpc.nio;

interface ChannelWriter {

    enum State {
        DISCONNECTED,
        CONNECTED
    }

    State getState();

    void write(Object message);

    void close();

}
