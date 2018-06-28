package in.xnnyygn.xraft.core.rpc.embedded;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.rpc.Endpoint;

public class EmbeddedEndpoint implements Endpoint {

    private final EmbeddedChannel channel;

    public EmbeddedEndpoint(EventBus eventBus) {
        this.channel = new EmbeddedChannel(eventBus);
    }

    public EmbeddedChannel getChannel() {
        return channel;
    }

}
