package in.xnnyygn.xraft.kvstore.command;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class GetCommand implements Serializable {

    private final String key;

    public GetCommand(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "GetCommand{" +
                "key='" + key + '\'' +
                '}';
    }

}
