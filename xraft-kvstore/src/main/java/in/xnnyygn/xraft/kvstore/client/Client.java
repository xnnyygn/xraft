package in.xnnyygn.xraft.kvstore.client;

import in.xnnyygn.xraft.kvstore.command.GetCommand;
import in.xnnyygn.xraft.kvstore.command.SetCommand;

public class Client {

    private final Selector selector;

    public Client(Selector selector) {
        this.selector = selector;
    }

    public void set(String key, Object value) {
        this.selector.send(new SetCommand(key, value));
    }

    public Object get(String key) {
        return this.selector.send(new GetCommand(key));
    }

}
