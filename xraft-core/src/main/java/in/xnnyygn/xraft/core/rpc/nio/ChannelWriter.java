package in.xnnyygn.xraft.core.rpc.nio;

@Deprecated
interface ChannelWriter {

    enum State {
        DISCONNECTED,
        CONNECTED
    }

    State getState();

    void write(Object message);

    void close();

}
