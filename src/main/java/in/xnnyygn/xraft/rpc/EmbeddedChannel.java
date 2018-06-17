package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.ServerId;

import java.util.ArrayList;
import java.util.List;

public class EmbeddedChannel implements Channel {

    private final List<ChannelListener> listeners = new ArrayList<>();

    @Override
    public void write(Object payload, ServerId senderId) {
        listeners.forEach((l) -> l.receive(payload, senderId));
    }

    public void addListener(ChannelListener listener) {
        this.listeners.add(listener);
    }

}
