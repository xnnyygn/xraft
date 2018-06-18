package in.xnnyygn.xraft.kvstore.command;

import java.io.Serializable;

public class SetCommand implements Serializable {

    private final String key;
    private final String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "SetCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

}
