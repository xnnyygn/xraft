package in.xnnyygn.xraft.kvstore.command;

import java.io.Serializable;

public class GetCommand implements Serializable {

    private final String key;

    public GetCommand(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
