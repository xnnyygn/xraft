package in.xnnyygn.xraft.kvstore.message;

import java.util.UUID;

public class GetCommand  {

    private final String requestId;
    private final String key;

    public GetCommand(String key) {
        this.requestId = UUID.randomUUID().toString();
        this.key = key;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "GetCommand{" +
                "key='" + key + '\'' +
                ", requestId='" + requestId + '\'' +
                '}';
    }
}
