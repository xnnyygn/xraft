package in.xnnyygn.xraft.kvstore.command;

import java.io.Serializable;

public class SetCommand implements Serializable {

    private final String key;
    private final Object value;

    public SetCommand(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

}
